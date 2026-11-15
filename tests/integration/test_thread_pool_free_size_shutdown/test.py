import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/thread_pools.xml"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# With max_*_thread_pool_free_size > 0 static thread pools keep threads.
# Meaning, we should always destroy static thread pools before calling GlobalThreadPool::shutdown
# in binaries that allow to configure max_*_thread_pool_free_size (i.e. server and local).
# These tests are verifying exactly that.


def test_server_graceful_shutdown_with_idle_pool_threads(started_cluster):
    def backup_destination(name):
        return (
            f"S3('http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/{name}',"
            f" 'minio', '{minio_secret_key}')"
        )

    # Force the usage of a static thread pool.
    node.query("CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO t VALUES (1)")
    node.query(f"BACKUP TABLE t TO {backup_destination('backup_server')}")

    node.stop_clickhouse()
    assert node.contains_in_log("Background threads finished")
    node.start_clickhouse()


def test_local_graceful_shutdown_with_idle_pool_threads(started_cluster):
    # Force the usage of a static thread pool in local.
    node.exec_in_container(
        [
            "clickhouse",
            "local",
            "--config-file",
            "/etc/clickhouse-server/config.d/thread_pools.xml",
            "--input_format_parallel_parsing=1",
            "--max_parsing_threads=4",
            "--query",
            "SELECT number FROM numbers(200000) INTO OUTFILE '/tmp/t.csv' FORMAT CSV;"
            " CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple();"
            " INSERT INTO t FROM INFILE '/tmp/t.csv' FORMAT CSV",
        ],
        timeout=10,
    )
