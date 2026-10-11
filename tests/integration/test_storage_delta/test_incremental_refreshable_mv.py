import logging
import os
import random
import string

import pyspark
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.s3_tools import LocalDownloader, LocalUploader
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config

cluster = ClickHouseCluster(__file__, with_spark=True)


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "/var/lib/clickhouse/user_files",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.master("local").getOrCreate()


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/enable_writes.xml",
            ],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip("DeltaLake engine is not available")

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )

        yield cluster

    finally:
        cluster.shutdown()


def create_source_table(instance, name):
    # MergeTree source with the block-number/offset columns the streaming cursor reads.
    instance.query(
        f"""
        CREATE TABLE {name} (k Int64)
        ENGINE = MergeTree ORDER BY k
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            add_minmax_index_for_block_number_column = 1,
            add_minmax_index_for_block_offset_column = 1,
            part_minmax_index_columns = 'with_block_number_offset'
        """
    )


def refresh(instance, mv):
    instance.query(f"SYSTEM REFRESH VIEW {mv}")
    instance.query(f"SYSTEM WAIT VIEW {mv}")


# Exactly-once incremental refreshable MV writing MergeTree -> a Delta Lake table created by ClickHouse,
# on local filesystem and S3 (MinIO). The cursor is a `domainMetadata` action in the same `_delta_log`
# commit as the data files. The database is Atomic, so there is NO Keeper coordination znode: after a
# restart, only the cursor in the Delta table can let the next round skip the rows already appended.
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_incremental_refreshable_mv_deltalake_exactly_once(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    suffix = randomize_table_name(storage_type + "_")
    src = f"irmv_src_{suffix}"
    tgt = f"irmv_tgt_{suffix}"
    mv = f"irmv_mv_{suffix}"

    if storage_type == "local":
        path = f"/var/lib/clickhouse/user_files/{tgt}"
        engine = f"DeltaLakeLocal('{path}')"
        delta_log = f"file('{path}/_delta_log/*.json', LineAsString)"
    else:
        url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{tgt}/"
        engine = f"DeltaLake('{url}', 'minio', '{minio_secret_key}')"
        delta_log = f"s3('{url}_delta_log/*.json', 'minio', '{minio_secret_key}', LineAsString)"

    create_source_table(instance, src)
    instance.query(
        f"CREATE TABLE {tgt} (k Int64) ENGINE = {engine} SETTINGS delta_lake_enable_domain_metadata = 1"
    )

    # REFRESH EVERY 10 YEAR + EMPTY: no automatic refresh; every refresh below is triggered manually.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {mv}
            REFRESH EVERY 10 YEAR APPEND INCREMENTAL
            TO {tgt} EMPTY
            AS SELECT k FROM {src}
        """
    )

    # Round 1: rows 0..4. The refresh commits them to the Delta log together with the cursor.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(5)")
    refresh(instance, mv)
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "5\t5"
    assert (
        instance.query(
            f"SELECT count() FROM {delta_log} "
            f"WHERE JSONExtractString(line, 'domainMetadata', 'domain') = 'clickhouse.refresh-cursor'"
        ).strip()
        == "1"
    ), "refresh cursor was not committed to the Delta log"

    # Restart wipes all in-memory RefreshTask state.
    instance.restart_clickhouse()

    # Round 2: rows 5..9. If the cursor survived (Delta log), only the 5 new rows are appended -> 10 rows,
    # 10 distinct (exactly-once). If it were lost, round 2 re-reads all 10 -> 15 rows.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(5, 5)")
    refresh(instance, mv)
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "10\t10"

    instance.query(f"DROP TABLE {mv}")
    instance.query(f"DROP TABLE {src}")
    instance.query(f"DROP TABLE {tgt}")


# Interop with Spark on a partitioned table that Spark created with the `domainMetadata` feature: the cursor
# goes through `DeltaLakePartitionedSink`, survives a Spark append and the checkpoint Spark writes for it,
# and Spark can still read the table that carries ClickHouse's domain.
def test_incremental_refreshable_mv_deltalake_spark_partitioned(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    suffix = randomize_table_name("spark_")
    src = f"irmv_src_{suffix}"
    tgt = f"irmv_tgt_{suffix}"
    mv = f"irmv_mv_{suffix}"
    path = f"/var/lib/clickhouse/user_files/{tgt}"

    # `delta.checkpointInterval = 1` makes every Spark commit also write a checkpoint.
    spark.sql(
        f"""
        CREATE TABLE delta.`{path}` (k BIGINT, p BIGINT) USING DELTA
        PARTITIONED BY (p)
        TBLPROPERTIES ('delta.feature.domainMetadata' = 'supported', 'delta.checkpointInterval' = '1')
        """
    )
    LocalUploader(instance).upload_directory(f"{path}/", f"{path}/")

    create_source_table(instance, src)
    instance.query(f"CREATE TABLE {tgt} ENGINE = DeltaLakeLocal('{path}')")
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {mv}
            REFRESH EVERY 10 YEAR APPEND INCREMENTAL
            TO {tgt} EMPTY
            AS SELECT k, k % 2 AS p FROM {src}
        """
    )

    # Round 1: rows 0..9 over two partitions.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(10)")
    refresh(instance, mv)
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "10\t10"

    # Spark reads ClickHouse's commit, then appends a row of its own, committing a new version and a checkpoint.
    LocalDownloader(instance).download_directory(f"{path}/", f"{path}/")
    assert spark.read.format("delta").load(path).count() == 10
    spark.sql(f"INSERT INTO delta.`{path}` VALUES (100, 0)")
    assert any(f.endswith(".checkpoint.parquet") for f in os.listdir(f"{path}/_delta_log"))
    LocalUploader(instance).upload_directory(f"{path}/", f"{path}/")

    # Round 2: rows 10..19. The cursor from round 1 survived the Spark commit and checkpoint, so only
    # the new rows are appended: 21 rows, 21 distinct. A lost cursor gives 31.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(10, 10)")
    refresh(instance, mv)
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "21\t21"

    LocalDownloader(instance).download_directory(f"{path}/", f"{path}/")
    assert spark.read.format("delta").load(path).count() == 21

    instance.query(f"DROP TABLE {mv}")
    instance.query(f"DROP TABLE {src}")
    instance.query(f"DROP TABLE {tgt}")
