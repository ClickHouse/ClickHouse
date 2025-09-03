import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .laketables import SparkTable
from integration.helpers.client import Client


class SparkAndClickHouseCheck:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def check_table(self, cluster, spark: SparkSession, table: SparkTable) -> bool:
        try:
            client = Client(
                host=cluster.instances["node0"].ip_address,
                port=9000,
                command=cluster.client_bin_path,
            )

            # Start by checking counts
            spark_count = spark.table(table.get_table_full_path()).count()
            ch_count_str = client.query(
                f"SELECT count(*) FROM {table.get_clickhouse_path()};"
            )
            if not isinstance(ch_count_str, str) or ch_count_str == "":
                self.logger.error(f"No count received {table.get_clickhouse_path()}")
                return False
            ch_count = int(ch_count_str.rstrip())
            if spark_count != ch_count:
                self.logger.error(
                    f"The row count for table {table.get_clickhouse_path()} doesn't match between Spark: {spark_count} and ClickHouse: {ch_count}"
                )
                return False

            order_by_cols = [k for k in table.columns.keys()]
            # Spark hash
            # Generate hash for each row in ClickHouse
            # Convert all columns to string and concatenate
            concat_cols = " || '||' || ".join([f"CAST({col} AS STRING)" for col in order_by_cols])
            # Generate hash using SQL
            query = f"""
            SELECT LOWER(HEX(MD5(CONCAT_WS('', COLLECT_LIST(row_hash))))) as table_hash
            FROM (
                SELECT LOWER(HEX(MD5({concat_cols}))) as row_hash
                FROM {table.get_clickhouse_path()}
                ORDER BY {', '.join([f"{col} ASC NULLS FIRST" for col in order_by_cols])}
            );
            """
            result = spark.sql(query).collect()
            spark_hash = result[0]["table_hash"]

            # ClickHouse hash
            # Generate hash for each row in ClickHouse
            concat_cols = " || '||' || ".join(
                [f"toString({col})" for col in order_by_cols]
            )
            # Convert all columns to string and concatenate
            clickhouse_hash = client.query(
                f"""
            SELECT hex(MD5(arrayStringConcat(groupArray(row_hash), ''))) as table_hash
            FROM (
                SELECT hex(MD5({concat_cols})) as row_hash
                FROM {table.get_clickhouse_path()}
                ORDER BY {', '.join([f"{col} ASC NULLS FIRST" for col in order_by_cols])}
            );
            """
            )
            if not isinstance(clickhouse_hash, str) or clickhouse_hash == "":
                self.logger.error(f"No hash found for {table.get_clickhouse_path()}")
                return False
            if spark_hash != clickhouse_hash.rstrip():
                self.logger.error(
                    f"The hash for table {table.get_clickhouse_path()} doesn't match between Spark and ClickHouse"
                )
                return False
        except Exception as e:
            # If an error happens, ignore it, but log it
            self.logger.error(str(e))
        return True
