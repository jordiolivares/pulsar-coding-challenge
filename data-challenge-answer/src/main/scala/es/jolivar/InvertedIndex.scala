package es.jolivar

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedSet

object InvertedIndex {

  type WordId = Long
  type WordText = String
  type DocumentId = Long
  type DocumentText = String

  case class Document(docId: DocumentId, text: DocumentText)
  case class Word(id: WordId, text: WordText)

  def getDocuments(pattern: String)(implicit sc: SparkContext): RDD[Document] = {
    // Note that this won't work with massive text files as they won't fit in the workers' memory
    sc.wholeTextFiles(pattern).map {
      case (filename, corpus) => Document(filename.split("/").last.toLong, corpus)
    }
  }

  def getDictionary(pattern: String)(implicit sc: SparkContext): RDD[Word] = {
    sc.textFile(pattern)
      .flatMap(_.trim.split("""\s+""")).filter(_.nonEmpty)
      .distinct
      .zipWithIndex
      .sortByKey()
      .map{ case (text, id) => Word(id, text) }
  }

  def computeInvertedIndex(lines: RDD[Document], dictionary: RDD[Word]): RDD[(WordId, SortedSet[DocumentId])] = {
    val joinableDictionary = dictionary.map { case Word(id, text) => (text, id) }

    lines
      .flatMap {case Document(docId, text) => text.trim.split("""\s+""").filter(_.nonEmpty).map(word => (word, docId)) }
      .join(joinableDictionary)
      .map { case (_, (docId, wordId)) => (wordId, docId)}
      .aggregateByKey(SortedSet[DocumentId]())((docs, newDoc) => docs + newDoc, (docsA, docsB) => docsA & docsB)
      .sortByKey()
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException(
      "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf()

    implicit val sc: SparkContext = new SparkContext(conf)

    println("Input is: " + inputPath)
    println("Output is: " + outputPath)

    val dictionary = getDictionary(inputPath).cache() // We will reuse this later so lets save reprocessing time

    dictionary
      .map{ case Word(id, text) =>
        text + " " + id
      }
      .saveAsTextFile(outputPath + "/dictionary")

    println("Saved dictionary")

    val docs = getDocuments(inputPath)

    val invertedIndex = computeInvertedIndex(docs, dictionary)

    invertedIndex
      .map { case (word, docsSet) => (word, "[" + docsSet.mkString(", ") + "]")}
      .saveAsTextFile(outputPath + "/inverted_index")

    println("Saved inverted index")
  }
}
