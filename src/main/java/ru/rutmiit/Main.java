package ru.rutmiit;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {
        // Создание сессии Spark
        SparkSession session = SparkSession.builder()
                .config("spark.driver.host", "localhost")
              .appName("DataFrame")
               .master("local[*]")
                .getOrCreate();
        DataFrameReader dataFrameReader = session.read();

        // Чтение данных из CSV-файла
        Dataset<Row> data = dataFrameReader.option("header", "true")
                .csv("data/titanic.csv");

        System.out.println("Средний возраст пассажиров:");
        data.select(avg("Age")).show();

        System.out.println("Количество пассажиров каждого класса:");
        data.groupBy("Pclass").count().show();

        System.out.println("Количество пассажиров по портам посадки:");
        data.groupBy("Embarked").count().show();

        System.out.println("10 самых дорогих билетов:");
        data.select("Fare", "Pclass", "Name").orderBy(desc("Fare")).limit(10).show();

        System.out.println("Количество пассажиров каждого пола:");
        data.groupBy("Sex").count().show();

        System.out.println("Средний возраст выживших и погибших пассажиров:");
        data.groupBy("Survived").avg("Age").show();

        System.out.println("Количество пассажиров с разным количеством родственников:");
        data.groupBy("SibSp", "Parch").count().show();

        System.out.println("10 самых дорогих билетов для первого класса:");
        data.filter(col("Pclass").equalTo(1))
            .select("Fare", "Pclass", "Name")
            .orderBy(desc("Fare"))
            .limit(10)
            .show();

        System.out.println("Топ 5 самых распространенных фамилий:");
        data.withColumn("Surname", split(col("Name"), ", ").getItem(0))
            .groupBy("Surname")
            .count()
            .orderBy(desc("count"))
            .limit(5)
            .show();

        System.out.println("Количество погибших пассажиров по портам:");
        data.filter(col("Survived").equalTo(0))
            .groupBy("Embarked")
            .count()
            .show();

        System.out.println("Количество выживших пассажиров каждого класса:");
        data.filter(col("Survived").equalTo(1))
            .groupBy("Pclass")
            .count()
            .show();

        System.out.println("Средняя стоимость билета для каждого класса:");
        data.groupBy("Pclass")
            .avg("Fare")
            .show();
    }
}