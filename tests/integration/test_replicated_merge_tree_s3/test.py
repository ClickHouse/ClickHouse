import logging
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

TABLE_NAME = "s3_test"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "2"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "3"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC = 1
FILES_OVERHEAD_METADATA_VERSION = 1
FILES_OVERHEAD_PER_PART_WIDE = (
    FILES_OVERHEAD_PER_COLUMN * 3
    + 2
    + 6
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
)
FILES_OVERHEAD_PER_PART_COMPACT = (
    10 + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC + FILES_OVERHEAD_METADATA_VERSION
)


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, additional_settings=None):
    settings = {
        "storage_policy": "s3",
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {TABLE_NAME} ON CLUSTER cluster(
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=ReplicatedMergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
        """

    list(cluster.instances.values())[0].query(create_table_statement)


def insert(cluster, node_idxs, verify=True):
    all_values = ""
    for node_idx in node_idxs:
        node = cluster.instances["node" + str(node_idx)]
        values = generate_values("2020-01-0" + str(node_idx), 4096)
        node.query(
            f"INSERT INTO {TABLE_NAME} VALUES {values}",
            settings={"insert_quorum": 3},
        )
        if node_idx != 1:
            all_values += ","
        all_values += values

    if verify:
        for node_idx in node_idxs:
            node = cluster.instances["node" + str(node_idx)]
            assert (
                node.query(
                    f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values",
                    settings={"select_sequential_consistency": 1},
                )
                == all_values
            )


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in list(cluster.instances.values()):
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    minio = cluster.minio_client
    # Remove extra objects to prevent tests cascade failing
    for obj in list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [(0, FILES_OVERHEAD_PER_PART_WIDE), (8192, FILES_OVERHEAD_PER_PART_COMPACT)],
)
def test_insert_select_replicated(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        additional_settings={"min_rows_for_wide_part": min_rows_for_wide_part},
    )

    insert(cluster, node_idxs=[1, 2, 3], verify=True)

    minio = cluster.minio_client
    assert len(
        list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    ) == 3 * (FILES_OVERHEAD + files_per_part * 3)


def test_drop_cache_on_cluster(cluster):
    create_table(
        cluster,
        additional_settings={"storage_policy": "s3_cache"},
    )

    insert(cluster, node_idxs=[1, 2, 3], verify=True)

    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    node3 = cluster.instances["node3"]

    node1.query(
        f"select * from clusterAllReplicas(cluster, default, {TABLE_NAME}) format Null"
    )

    assert int(node1.query("select count() from system.filesystem_cache")) > 0
    assert int(node2.query("select count() from system.filesystem_cache")) > 0
    assert int(node3.query("select count() from system.filesystem_cache")) > 0

    node1.query("system drop filesystem cache on cluster cluster")

    assert int(node1.query("select count() from system.filesystem_cache")) == 0
    assert int(node2.query("select count() from system.filesystem_cache")) == 0
    assert int(node3.query("select count() from system.filesystem_cache")) == 0
