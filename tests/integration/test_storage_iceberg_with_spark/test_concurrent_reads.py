from multiprocessing import Pool
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    execute_spark_query_general,
    get_creation_expression,

)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_concurrent_reads(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_concurrent_reads_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )
    
    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    num_insert_threads = 10
    num_select_threads = 25
    has_errors : bool = False
    batch_size = 50

    def run_concurrent_queries(i):
        def select(_):
            try:
                result = instance.query(f"SELECT * FROM {TABLE_NAME}")
                assert len(result.split("\n")) % batch_size == 0
            except:
                has_errors = True

        def insert(_):
            try:
                execute_spark_query(
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

        num_rows = num_insert_threads * batch_size * (i + 1)
        assert num_rows == int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME}",
            )
        )

    for i in range(5):
        run_concurrent_queries(i)