import logging
import random
import string
import time

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
FILES_OVERHEAD_PER_PART_WIDE = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6 + 1
FILES_OVERHEAD_PER_PART_COMPACT = 10 + 1


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, additional_settings=None):
    create_table_statement = """
        CREATE TABLE s3_test ON CLUSTER cluster(
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
        create_table_statement += ","
        create_table_statement += additional_settings

    list(cluster.instances.values())[0].query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in list(cluster.instances.values()):
        node.query("DROP TABLE IF EXISTS s3_test")

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
        additional_settings="min_rows_for_wide_part={}".format(min_rows_for_wide_part),
    )

    all_values = ""
    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        values = generate_values("2020-01-0" + str(node_idx), 4096)
        node.query(
            "INSERT INTO s3_test VALUES {}".format(values),
            settings={"insert_quorum": 3},
        )
        if node_idx != 1:
            all_values += ","
        all_values += values

    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        assert (
            node.query(
                "SELECT * FROM s3_test order by dt, id FORMAT Values",
                settings={"select_sequential_consistency": 1},
            )
            == all_values
        )

    minio = cluster.minio_client
    assert len(
        list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    ) == (3 * FILES_OVERHEAD) + (files_per_part * 3)


def test_drop_table(cluster):
    node = list(cluster.instances.values())[0]
    node2 = list(cluster.instances.values())[1]
    node.query(
        "create table test_drop_table (n int) engine=ReplicatedMergeTree('/test/drop_table', '1') order by n partition by n % 99 settings storage_policy='s3'"
    )
    node2.query(
        "create table test_drop_table (n int) engine=ReplicatedMergeTree('/test/drop_table', '2') order by n partition by n % 99 settings storage_policy='s3'"
    )
    node.query("insert into test_drop_table select * from numbers(1000)")
    node2.query("system sync replica test_drop_table")

    with PartitionManager() as pm:
        pm._add_rule(
            {
                "probability": 0.01,
                "destination": node.ip_address,
                "source_port": 2181,
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        pm._add_rule(
            {
                "probability": 0.01,
                "source": node.ip_address,
                "destination_port": 2181,
                "action": "REJECT --reject-with tcp-reset",
            }
        )

        # Will drop in background with retries
        node.query("drop table test_drop_table")

        # It should not be possible to create a replica with the same path until the previous one is completely dropped
        for i in range(0, 100):
            node.query_and_get_answer_with_error(
                "create table if not exists test_drop_table (n int) "
                "engine=ReplicatedMergeTree('/test/drop_table', '1') "
                "order by n partition by n % 99 settings storage_policy='s3'"
            )
            time.sleep(0.2)

    # Wait for drop to actually finish
    node.wait_for_log_line(
        "Removing metadata /var/lib/clickhouse/metadata_dropped/default.test_drop_table",
        timeout=60,
        look_behind_lines=1000000,
    )

    # It could leave some leftovers, remove them
    replicas = node.query_with_retry(
        "select name from system.zookeeper where path='/test/drop_table/replicas'"
    )
    if "1" in replicas and "test_drop_table" not in node.query("show tables"):
        node2.query("system drop replica '1' from table test_drop_table")

    # Just in case table was not created due to connection errors
    node.query(
        "create table if not exists test_drop_table (n int) engine=ReplicatedMergeTree('/test/drop_table', '1') "
        "order by n partition by n % 99 settings storage_policy='s3'"
    )

    # A table may get stuck in readonly mode if zk connection was lost during CREATE
    node.query("detach table test_drop_table sync")
    node.query("attach table test_drop_table")

    node.query_with_retry(
        "system sync replica test_drop_table",
        settings={"receive_timeout": 5},
        sleep_time=5,
        retry_count=10,
    )
    node2.query("drop table test_drop_table sync")
    assert "1000\t499500\n" == node.query(
        "select count(n), sum(n) from test_drop_table"
    )
    node.query("drop table test_drop_table sync")
