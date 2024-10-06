import logging
import random
import string
import time
from operator import itemgetter

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf.xml"],
            user_configs=["configs/config.d/users.xml"],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            user_configs=["configs/config.d/users.xml"],
            macros={"replica": "2"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/config.d/storage_conf.xml"],
            user_configs=["configs/config.d/users.xml"],
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
GC_SLEEP_INTERVAL = 1.5


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=itemgetter(0))
    return ",".join(f"('{x}',{y},'{z}')" for x, y, z in data)


def list_objects(cluster, path="data/", hint="list_objects"):
    minio = cluster.minio_client
    objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    logging.info(f"{hint} ({len(objects)}): {[x.object_name for x in objects]}")
    return objects


def get_node(cluster, idx):
    return list(cluster.instances.values())[idx]


def create_table(cluster, additional_settings=None):
    create_table_statement = """
        CREATE TABLE s3_test ON CLUSTER cluster (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=ReplicatedMergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS storage_policy='s3'
        """
    if additional_settings:
        create_table_statement += f",{additional_settings}"

    get_node(cluster, 0).query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in list(cluster.instances.values()):
        node.query("DROP TABLE IF EXISTS s3_test SYNC")
        node.query("DROP TABLE IF EXISTS test_drop_table SYNC")
    
    time.sleep(1.5)

    minio = cluster.minio_client
    # Remove extra objects to prevent tests cascade failing
    for obj in list_objects(cluster):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [
        (0, FILES_OVERHEAD_PER_PART_WIDE),
        (8192, FILES_OVERHEAD_PER_PART_COMPACT)
    ],
)
def test_insert_select_replicated(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        additional_settings=f"min_rows_for_wide_part={min_rows_for_wide_part}",
    )

    # Insert different data to each replica
    all_values = ""
    for node_idx in range(1, 4):
        node = cluster.instances[f"node{node_idx}"]
        values = generate_values(f"2020-01-0{node_idx}", 4096)
        node.query(
            f"INSERT INTO s3_test VALUES {values}",
            settings={"insert_quorum": 3},
        )
        if node_idx != 1:
            all_values += ","
        all_values += values

    # Check that each replica has all replicated data
    for node_idx in range(1, 4):
        node = cluster.instances[f"node{node_idx}"]
        assert (
            node.query(
                "SELECT * FROM s3_test order by dt, id FORMAT Values",
                settings={"select_sequential_consistency": 1},
            )
            == all_values
        )
    
    # Wait at least one GC iteration
    time.sleep(1.5)    

    assert len(list_objects(cluster, "snapshot/")) == 1
    assert len(list_objects(cluster, "data/")) == (3 * FILES_OVERHEAD) + (files_per_part * 3)

    get_node(cluster, 0).query("drop table s3_test SYNC")
    get_node(cluster, 1).query("drop table s3_test SYNC")

    # Wait at least one GC iteration
    time.sleep(1.5)    

    assert len(list_objects(cluster, "data/")) == (1 * FILES_OVERHEAD) + (files_per_part * 3)

    get_node(cluster, 2).query("drop table s3_test SYNC")

    # Wait at least one GC iteration
    time.sleep(1.5)    

    assert len(list_objects(cluster, "data/")) == 0

