from multiprocessing.dummy import Pool

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

    spark.conf.set("spark.sql.iceberg.commit.sync", "true")
    
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

    # Keep the concurrency width wide enough for inserts and selects to interleave
    # and surface read/commit races. The per-insert row count and the number of
    # outer rounds dominate wall time without adding coverage, so they are small.
    num_insert_threads = 15
    num_select_threads = 25
    batch_size = 5

    def run_concurrent_queries(i):
        def select(_):
            # Bound per-query fan-out so the concurrent readers don't exhaust the global thread pool.
            instance.query(
                f"SELECT * FROM {TABLE_NAME}",
                settings={"max_threads": 4},
            )
                

        def insert(_):
            while True:
                try:
                    spark.sql(
                        f"""
                            INSERT INTO {TABLE_NAME} 
                            SELECT id as number 
                            FROM range({batch_size})
                        """
                )
                except Exception as e:
                    if "CommitFailedException" in str(e):
                        continue
                    else:
                        raise
                else:
                    break
        
        insert_pool = Pool(num_insert_threads)
        select_pool = Pool(num_select_threads)
        insert_async = insert_pool.map_async(insert, range(num_insert_threads))
        select_async = select_pool.map_async(select, range(num_select_threads))
        
        try:
            insert_async.get()
            select_async.get()
        except Exception as e:
            raise e
        finally:
            insert_pool.close()
            select_pool.close()
            insert_pool.join()
            select_pool.join()
    
        expected_rows = num_insert_threads * batch_size * (i + 1)
        rows_in_ch = int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME}",
            )
        )

        assert rows_in_ch == expected_rows, f"Expected {expected_rows} rows, but got {rows_in_ch}"

    for i in range(3):
        run_concurrent_queries(i)