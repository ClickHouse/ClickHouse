from multiprocessing.dummy import Pool
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


def test_concurrent_reads(started_cluster_iceberg):
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session
    storage_type = 's3'

    TABLE_NAME = (
        "test_concurrent_reads_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg)

    num_insert_threads = 15
    num_select_threads = 25
    has_errors : bool = False
    batch_size = 50

    def run_concurrent_queries(i):
        def select(_):
            try:
                result = instance.query(
                    f"SELECT * FROM {TABLE_NAME}",
                )
                assert len(result.split("\n")) % batch_size == 0
            except:
                has_errors = True

        def insert(_):
            try:
                spark.sql(
                    f"""
                        INSERT INTO {TABLE_NAME} 
                        SELECT id as number 
                        FROM range({batch_size})
                    """
                )
            except:
                has_errors = True

        select_pool = Pool(num_select_threads)
        insert_pool = Pool(num_insert_threads)
        sp = select_pool.map_async(select, range(num_select_threads))
        ip = insert_pool.map_async(insert, range(num_insert_threads))
        sp.wait()
        ip.wait()

        assert not has_errors

        expected_rows = num_insert_threads * batch_size * (i + 1)
        kek = spark.sql(
            f"""
                SELECT count(*) FROM {TABLE_NAME}
            """
        )
        kek.show(truncate=False)
        rows_in_spark = kek.collect()[0]["count(1)"]
        rows_in_ch = int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME}",
            )
        )

        if rows_in_spark != expected_rows or rows_in_ch != expected_rows:
            assert (rows_in_spark == expected_rows) and (rows_in_ch == expected_rows), f"Iteration {i} failed, expected {expected_rows}, got {rows_in_spark} in spark and {rows_in_ch} in CH"

    for i in range(10):
        run_concurrent_queries(i)