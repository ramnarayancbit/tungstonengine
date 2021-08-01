package com.test.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object RDDJOINDemo {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Counter").setMaster("local");
    val sc = new SparkContext(conf)
    //val textFile = sc.textFile("file:///C:/Users/mine/Desktop/BIGDATA/SPARK/DATA/SpeedDating.csv")
    
     val englishRDD = sc.makeRDD(List((1,"one"),(2,"two"))).setName("englishRDD").cache
val spanishRDD = sc.makeRDD(List((1,"uno"),(2,"dos"))).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
val joinedMultiLangRDD = englishRDD.join(spanishRDD)
val unionMultiLangRDD = englishRDD.union(spanishRDD)
val twoFilteredRDD = joinedMultiLangRDD.mapValues(definitions=>definitions.productIterator.toList.mkString(","))
                  .filter(keyValue=>keyValue._1 == 2)
twoFilteredRDD.collect
unionMultiLangRDD.reduceByKeyLocally((words, word)=> s"$words,$word")
joinedMultiLangRDD.foreach(println)
    
    //sortedCounts.saveAsTextFile("file:///C:/Users/mine/Desktop/BIGDATA/SPARK/DATA/9")
  }
}