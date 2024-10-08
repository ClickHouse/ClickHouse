import logging
import os
import time

import pytest
from pyhdfs import HdfsClient

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.utility import generate_values
from helpers.wait_for_helpers import (
    wait_for_delete_empty_parts,
    wait_for_delete_inactive_parts,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(
    SCRIPT_DIR, "./_instances/node/configs/config.d/storage_conf.xml"
)


if is_arm():
    pytestmark = pytest.mark.skip


def create_table(cluster, table_name, additional_settings=None):
    node = cluster.instances["node"]

    create_table_statement = """
        CREATE TABLE {} (
            dt Date, id Int64, data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS
            storage_policy='hdfs',
            old_parts_lifetime=0,
            index_granularity=512,
            temporary_directories_lifetime=1
        """.format(
        table_name
    )

    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings

    node.query(create_table_statement)


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


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node", main_configs=["configs/config.d/storage_conf.xml"], with_hdfs=True
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        fs = HdfsClient(hosts=cluster.hdfs_ip)
        fs.mkdirs("/clickhouse")

        logging.info("Created HDFS directory")

        yield cluster
    finally:
        cluster.shutdown()


def wait_for_delete_hdfs_objects(cluster, expected, num_tries=30):
    fs = HdfsClient(hosts=cluster.hdfs_ip)
    while num_tries > 0:
        num_hdfs_objects = len(fs.listdir("/clickhouse"))
        if num_hdfs_objects == expected:
            break
        num_tries -= 1
        time.sleep(1)
    assert len(fs.listdir("/clickhouse")) == expected


@pytest.fixture(autouse=True)
def drop_table(cluster):
    node = cluster.instances["node"]

    fs = HdfsClient(hosts=cluster.hdfs_ip)
    hdfs_objects = fs.listdir("/clickhouse")
    print("Number of hdfs objects to delete:", len(hdfs_objects), sep=" ")

    node.query("DROP TABLE IF EXISTS hdfs_test SYNC")

    try:
        wait_for_delete_hdfs_objects(cluster, 0)
    finally:
        hdfs_objects = fs.listdir("/clickhouse")
        if len(hdfs_objects) == 0:
            return
        print(
            "Manually removing extra objects to prevent tests cascade failing: ",
            hdfs_objects,
        )
        for path in hdfs_objects:
            fs.delete(path)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [(0, FILES_OVERHEAD_PER_PART_WIDE), (8192, FILES_OVERHEAD_PER_PART_COMPACT)],
)
def test_simple_insert_select(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        "hdfs_test",
        additional_settings="min_rows_for_wide_part={}".format(min_rows_for_wide_part),
    )

    node = cluster.instances["node"]

    values1 = generate_values("2020-01-03", 4096)
    node.query("INSERT INTO hdfs_test VALUES {}".format(values1))
    assert (
        node.query("SELECT * FROM hdfs_test order by dt, id FORMAT Values") == values1
    )

    fs = HdfsClient(hosts=cluster.hdfs_ip)

    hdfs_objects = fs.listdir("/clickhouse")
    print(hdfs_objects)
    assert len(hdfs_objects) == FILES_OVERHEAD + files_per_part

    values2 = generate_values("2020-01-04", 4096)
    node.query("INSERT INTO hdfs_test VALUES {}".format(values2))
    assert (
        node.query("SELECT * FROM hdfs_test ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + files_per_part * 2

    assert (
        node.query("SELECT count(*) FROM hdfs_test where id = 1 FORMAT Values") == "(2)"
    )


def test_alter_table_columns(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip)

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(
            generate_values("2020-01-03", 4096, -1)
        )
    )

    node.query("ALTER TABLE hdfs_test ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have merged
    node.query("OPTIMIZE TABLE hdfs_test")

    assert node.query("SELECT sum(col1) FROM hdfs_test FORMAT Values") == "(8192)"
    assert (
        node.query("SELECT sum(col1) FROM hdfs_test WHERE id > 0 FORMAT Values")
        == "(4096)"
    )
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN,
    )

    node.query(
        "ALTER TABLE hdfs_test MODIFY COLUMN col1 String",
        settings={"mutations_sync": 2},
    )

    assert node.query("SELECT distinct(col1) FROM hdfs_test FORMAT Values") == "('1')"
    # and file with mutation
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN + 1,
    )

    node.query("ALTER TABLE hdfs_test DROP COLUMN col1", settings={"mutations_sync": 2})

    # and 2 files with mutations
    wait_for_delete_hdfs_objects(
        cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + 2
    )


def test_attach_detach_partition(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip)

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE hdfs_test DETACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 2
        - FILES_OVERHEAD_METADATA_VERSION,
    )

    node.query("ALTER TABLE hdfs_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE hdfs_test DROP PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query("ALTER TABLE hdfs_test DETACH PARTITION '2020-01-04'")
    node.query(
        "ALTER TABLE hdfs_test DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD)


def test_move_partition_to_another_disk(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip)

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-04' TO DISK 'hdfs'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2


def test_table_manipulations(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip)

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )

    node.query("RENAME TABLE hdfs_test TO hdfs_renamed")
    assert node.query("SELECT count(*) FROM hdfs_renamed FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("RENAME TABLE hdfs_renamed TO hdfs_test")
    assert node.query("CHECK TABLE hdfs_test FORMAT Values") == "(1)"

    node.query("DETACH TABLE hdfs_test")
    node.query("ATTACH TABLE hdfs_test")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("TRUNCATE TABLE hdfs_test")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD)


def test_move_replace_partition_to_another_table(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip)

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(
            generate_values("2020-01-05", 4096, -1)
        )
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(
            generate_values("2020-01-06", 4096, -1)
        )
    )
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(16384)"

    hdfs_objects = fs.listdir("/clickhouse")
    assert len(hdfs_objects) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4

    create_table(cluster, "hdfs_clone")

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-03' TO TABLE hdfs_clone")
    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-05' TO TABLE hdfs_clone")
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(id) FROM hdfs_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_clone FORMAT Values") == "(8192)"

    # Number of objects in HDFS should be unchanged.
    hdfs_objects = fs.listdir("/clickhouse")
    for obj in hdfs_objects:
        print("Object in HDFS after move", obj)
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    # Add new partitions to source table, but with different values and replace them from copied table.
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(
            generate_values("2020-01-03", 4096, -1)
        )
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(16384)"

    hdfs_objects = fs.listdir("/clickhouse")
    for obj in hdfs_objects:
        print("Object in HDFS after insert", obj)

    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 6
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("ALTER TABLE hdfs_test REPLACE PARTITION '2020-01-03' FROM hdfs_clone")
    node.query("ALTER TABLE hdfs_test REPLACE PARTITION '2020-01-05' FROM hdfs_clone")
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(16384)"
    assert node.query("SELECT sum(id) FROM hdfs_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_clone FORMAT Values") == "(8192)"

    # Wait for outdated partitions deletion.
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("DROP TABLE hdfs_clone SYNC")
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(16384)"

    # Data should remain in hdfs
    hdfs_objects = fs.listdir("/clickhouse")

    for obj in hdfs_objects:
        print("Object in HDFS after drop", obj)

    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )
