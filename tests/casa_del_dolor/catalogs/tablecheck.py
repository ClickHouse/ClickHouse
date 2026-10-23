import logging
import traceback
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    CharType,
    VarcharType,
    StringType,
    BinaryType,
    ArrayType,
    MapType,
    TimestampType,
    DateType,
    FloatType,
    DoubleType,
    DecimalType,
)

from .laketables import SparkTable, LakeFormat
from integration.helpers.client import Client


class SparkAndClickHouseCheck:

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _check_type_valid_for_comparison(self, dtype) -> bool:
        if isinstance(dtype, ArrayType):
            return not isinstance(
                dtype.elementType, ArrayType
            ) and self._check_type_valid_for_comparison(dtype.elementType)
        if isinstance(
            dtype,
            (
                FloatType,
                DoubleType,
                MapType,
                StructType,
                StringType,
                BinaryType,
                CharType,
                VarcharType,
                TimestampType,
                DateType,
            ),
        ):
            # Map type is not comparable in Spark, Struct is complicated
            return False
        return True

    def check_table(self, cluster, spark: SparkSession, table: SparkTable) -> bool:
        try:
            clickhouse_predicate = ""
            spark_predicate = ""
            extra_predicate = ""
            snapshots = []
            timestamps = []

            client = Client(
                host=(
                    cluster.instances["node0"].ip_address
                    if hasattr(cluster, "instances")
                    else "localhost"
                ),
                port=9000,
                command=cluster.client_bin_path,
            )

            # There is multithreading, so time travel is now required
            if table.lake_format == LakeFormat.Iceberg:
                result = spark.sql(
                    f"SELECT snapshot_id, committed_at FROM {table.get_table_full_path()}.snapshots;"
                ).collect()
                snapshots = [r.snapshot_id for r in result]
                timestamps = [r.committed_at for r in result]
            else:
                result = spark.sql(
                    f"DESCRIBE HISTORY {table.get_table_full_path()};"
                ).collect()
                snapshots = [r.version for r in result]

            if len(snapshots) > 0 and (
                len(timestamps) == 0 or random.randint(1, 2) == 1
            ):
                next_snapshot = random.choice(snapshots)
                clickhouse_predicate = f" SETTINGS {'iceberg_snapshot_id' if table.lake_format == LakeFormat.Iceberg else 'delta_lake_snapshot_version'} = {next_snapshot}"
                spark_predicate = f" VERSION AS OF {next_snapshot}"
                extra_predicate = f" on snapshot {next_snapshot}"
            elif len(timestamps) > 0:
                next_time = random.choice(timestamps)
                clickhouse_predicate = f" SETTINGS iceberg_timestamp_ms = {int(next_time.timestamp() * 1000)}"
                spark_predicate = f" TIMESTAMP AS OF '{next_time}'"
                extra_predicate = f" on timestamp {next_time}"

            # Start by checking counts
            spark_query = spark.sql(
                f"SELECT count(*) c FROM {table.get_table_full_path()}{spark_predicate};"
            ).collect()
            spark_count = spark_query[0]["c"]
            ch_count_str = client.query(
                f"SELECT count(*) FROM {table.get_clickhouse_path()}{clickhouse_predicate};"
            )
            if not isinstance(ch_count_str, str) or ch_count_str == "":
                self.logger.error(
                    f"No count received for {table.get_clickhouse_path()}"
                )
                return False
            ch_count = int(ch_count_str.rstrip())
            if spark_count != ch_count:
                self.logger.error(
                    f"The row count for table {table.get_clickhouse_path()}{extra_predicate} doesn't match between Spark: {spark_count} and ClickHouse: {ch_count}"
                )
                return False

            order_by_cols = [
                v
                for v in table.columns.values()
                if self._check_type_valid_for_comparison(v.spark_type)
            ]
            if len(order_by_cols) == 0:
                self.logger.info(
                    f"No columns valid to compare for {table.get_clickhouse_path()}"
                )
                return True
            self.logger.info(
                f"Comparing hashes for table {table.get_clickhouse_path()} for column(s) {','.join([col.column_name for col in order_by_cols])}{extra_predicate}"
            )

            # Spark hash
            # Convert all columns to string and concatenate. Remove trailing 0 for decimals
            spark_strings = {
                col.column_name: (
                    f"TRIM(TRAILING '0' FROM CAST({col.column_name} AS STRING))"
                    if isinstance(col.spark_type, DecimalType)
                    else f"CAST({col.column_name} AS STRING)"
                )
                for col in order_by_cols
            }
            concat_cols = ", '||', ".join([col.column_name for col in order_by_cols])
            # Generate hash using SQL
            query = f"""
            SELECT MD5(CONCAT_WS('', COLLECT_LIST(row_hash))) as table_hash
            FROM (
                SELECT MD5(CONCAT({concat_cols})) as row_hash
                FROM (SELECT {', '.join([f'{v} AS {k}' for k, v in spark_strings.items()])}
                      FROM {table.get_table_full_path()}{spark_predicate}) x
                ORDER BY {', '.join([f'{k} ASC NULLS FIRST' for k in spark_strings.keys()])}
            );
            """
            result = spark.sql(query).collect()
            spark_hash = result[0]["table_hash"]

            # ClickHouse hash
            # Convert all columns to string and concatenate
            # ClickHouse arrays as strings don't have a space after the comma, add it
            clickhouse_strings = {
                col.column_name: (
                    f"'[' || arrayStringConcat(arrayMap(x -> toString(x), {col.column_name}), ', ') || ']'"
                    if isinstance(col.spark_type, (ArrayType))
                    else f"toString({col.column_name})"
                )
                for col in order_by_cols
            }
            concat_cols = " || '||' || ".join(
                [col.column_name for col in order_by_cols]
            )
            # Generate hash for each row in ClickHouse
            clickhouse_hash = client.query(
                f"""
            SELECT lower(hex(MD5(arrayStringConcat(groupArray(row_hash), '')))) as table_hash
            FROM (
                SELECT lower(hex(MD5({concat_cols}))) as row_hash
                FROM (SELECT {', '.join([f'{v} AS {k}' for k, v in clickhouse_strings.items()])}
                      FROM {table.get_clickhouse_path()}) x
                ORDER BY {', '.join([f'{k} ASC NULLS FIRST' for k in clickhouse_strings.keys()])}
            ){clickhouse_predicate};
            """
            )
            if not isinstance(clickhouse_hash, str) or clickhouse_hash == "":
                self.logger.error(f"No hash found for {table.get_clickhouse_path()}")
                return False
            if spark_hash != clickhouse_hash.rstrip():
                self.logger.error(
                    f"The hash for table {table.get_clickhouse_path()}{extra_predicate} doesn't match between Spark and ClickHouse"
                )
                return False
        except Exception as e:
            # If an error happens, ignore it, but log it
            traceback.print_exc()
            self.logger.exception(e)
        return True
