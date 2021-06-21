from pyspark.sql import SparkSession
import os


class SparkSessionBuilder:
    def __init__(self):
        pass

    def sessionbuilder(self, app_name="Test"):
        real_path = os.path.dirname(os.path.realpath(__file__))
        hadoop_jar_path = os.path.join(real_path, 'resources/gcs-connector-hadoop2-latest.jar')
        spark_session = SparkSession \
            .builder \
            .master('local[*]') \
            .appName(app_name) \
            .config("spark.jars", hadoop_jar_path)\
            .getOrCreate()
        return spark_session

    def csv_parser(self, spark_session, file_path="sample.csv", header=False):

        df = spark_session.read.load(file_path, format="csv", header=header)
        return df

    def json_parser(self, spark_session, file_path="sample.json", header=False):

        df = spark_session.read.load(file_path, format="json", header=header)
        return df


if __name__ == "__main__":
    builder = SparkSessionBuilder()
    spark_session = builder.sessionbuilder(app_name="Builder")
    df = builder.csv_parser(spark_session, header=True)
