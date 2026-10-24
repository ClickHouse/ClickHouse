"""paimonHDFS coverage, riding the HDFS namenode embedded in the Hive test
image (service `hdfs1`, `fs.defaultFS = hdfs://hivetest:9000`) — the same
service test_hive_query uses. The table is written locally by Spark and pushed
into HDFS with `hdfs dfs -put` inside the container (the image ships the
Hadoop CLI), so no HDFS client is needed on the runner.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster, is_arm, run_and_check
from helpers.paimon_utils import (
    create_spark_paimon_table,
    get_schema_and_data,
    get_spark_paimon,
    get_uuid_str,
    spark_table,
)

# The `lgboustc/hive_test` image is amd64-only; on ARM runners the Hadoop JVM
# stack under QEMU never becomes reachable in budget (see test_hive_query).
if is_arm():
    pytestmark = pytest.mark.skip

HDFS_URL = "hdfs://hivetest:9000"


@pytest.fixture(scope="module")
def started_cluster_paimon_hdfs():
    try:
        cluster = ClickHouseCluster(__file__, name="paimon-hdfs")
        cluster.add_instance("node1", with_hive=True, stay_alive=True)
        cluster.start()
        cluster.spark_session = get_spark_paimon(cluster.instances_dir)
        yield cluster
    finally:
        cluster.shutdown()


def _upload_to_hdfs(started_cluster, table_name):
    local_dir = f"/var/lib/clickhouse/user_files/paimon_data/default.db/{table_name}"
    assert os.path.isdir(local_dir)
    hdfs_container = started_cluster.get_instance_docker_id("hdfs1")
    run_and_check(["docker", "cp", local_dir, f"{hdfs_container}:/tmp/{table_name}"])
    run_and_check(
        [
            "docker",
            "exec",
            hdfs_container,
            "bash",
            "-c",
            f"hdfs dfs -mkdir -p /paimon/default.db && "
            f"hdfs dfs -put /tmp/{table_name} /paimon/default.db/{table_name} && "
            f"rm -rf /tmp/{table_name}",
        ]
    )
    return f"{HDFS_URL}/paimon/default.db/{table_name}"


def test_paimon_hdfs(started_cluster_paimon_hdfs):
    started_cluster = started_cluster_paimon_hdfs
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_paimon_hdfs_" + get_uuid_str()

    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, b STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')")
    hdfs_url = _upload_to_hdfs(started_cluster, table_name)

    # Table function.
    table_expr = f"paimonHDFS('{hdfs_url}')"
    schema, data = get_schema_and_data(instance, table_expr, order_by="a")
    assert schema == [["a", "Int32"], ["b", "Nullable(String)"]]
    assert data == [["1", "one"], ["2", "two"], ["3", "three"]]

    # Engine.
    ch_table = "paimon_hdfs_engine_" + get_uuid_str()
    instance.query(
        f"CREATE TABLE {ch_table} ENGINE = PaimonHDFS('{hdfs_url}')",
        settings={"allow_experimental_paimon_storage_engine": 1},
    )
    try:
        assert (
            instance.query(f"SELECT a, b FROM {ch_table} ORDER BY a")
            == "1\tone\n2\ttwo\n3\tthree\n"
        )
    finally:
        instance.query(f"DROP TABLE {ch_table} SYNC")
