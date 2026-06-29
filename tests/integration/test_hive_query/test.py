import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "h0_0_0",
            main_configs=["configs/config.xml"],
            extra_configs=["configs/hdfs-site.xml"],
            with_hive=True,
        )

        logging.info("Starting cluster ...")
        cluster.start()

        hive_container = cluster.get_instance_docker_id("hdfs1")
        cluster.copy_file_to_container(
            hive_container,
            os.path.join(SCRIPT_DIR, "data", "prepare_hive_data.sh"),
            "/prepare_hive_data.sh",
        )
        # The Hive metastore / HiveServer2 may still be starting up; retry the data
        # preparation (the script is idempotent) until the hive CLI can talk to it.
        last_error = None
        for _ in range(20):
            try:
                cluster.exec_in_container(
                    hive_container, ["bash", "-c", "bash /prepare_hive_data.sh"]
                )
                last_error = None
                break
            except Exception as e:  # noqa: BLE001
                last_error = e
                time.sleep(15)
        if last_error is not None:
            raise last_error
        yield cluster
    finally:
        cluster.shutdown()


def query_with_retry(node, query, retries=20, sleep=10):
    # The Hive metastore can take a while to accept connections after the cluster
    # starts; StorageHive connects lazily on the first read, so retry until it succeeds.
    last_error = None
    for _ in range(retries):
        try:
            return node.query(query)
        except Exception as e:  # noqa: BLE001
            last_error = e
            time.sleep(sleep)
    raise last_error


def test_select_without_where(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/102636:
    # `SELECT * FROM hive_table` without a WHERE clause passed a null filter to
    # StorageHive, which dereferenced it and terminated the server with a
    # segmentation fault. A query with a WHERE clause happened to avoid the crash.
    node = started_cluster.instances["h0_0_0"]

    node.query("DROP TABLE IF EXISTS default.hive_demo")
    node.query(
        """
        CREATE TABLE default.hive_demo (`id` Nullable(String), `score` Nullable(Int32), `day` Nullable(String))
        ENGINE = Hive('thrift://hivetest:9083', 'test', 'demo') PARTITION BY(day)
        """
    )

    # No WHERE clause: this used to crash the server.
    assert query_with_retry(node, "SELECT count(*) FROM default.hive_demo").strip() == "4"
    assert (
        node.query("SELECT id, score, day FROM default.hive_demo ORDER BY day, id")
        == "a\t1\t2021-11-01\nb\t2\t2021-11-05\nc\t3\t2021-11-05\nd\t4\t2021-11-11\n"
    )

    # A read with a filter exercises partition (and split) pruning ...
    assert (
        node.query(
            "SELECT count(*) FROM default.hive_demo WHERE day = '2021-11-05'"
        ).strip()
        == "2"
    )

    # ... and must not leave stale pruning state on the process-wide cached hive file:
    # a subsequent unfiltered read of the same table still returns all rows.
    assert node.query("SELECT count(*) FROM default.hive_demo").strip() == "4"
    assert node.query("SELECT count(*) FROM default.hive_demo WHERE day = '2021-11-01'").strip() == "1"
    assert node.query("SELECT count(*) FROM default.hive_demo").strip() == "4"
