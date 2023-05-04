import time
import os
import pathlib
import logging
import shutil

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.utility import generate_values
from helpers.wait_for_helpers import wait_for_delete_inactive_parts
from helpers.wait_for_helpers import wait_for_delete_empty_parts


remote_disk_path = pathlib.Path(__file__).parent / "_instances" / "server" / "database" / "remote_disk"


def files_number_in_disk():
    return len([p.relative_to(remote_disk_path) for p in remote_disk_path.rglob("*") if p.is_file()])


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def create_table(cluster, table_name, additional_settings=None):
    node = cluster.instances["client"]

    create_table_statement = """
        CREATE TABLE {} (
            dt Date, id Int64, data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS
            storage_policy='remote_disk',
            old_parts_lifetime=0,
            index_granularity=512
        """.format(
        table_name
    )

    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings

    node.query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    node = cluster.instances["client"]

    node.query("DROP TABLE IF EXISTS remote_disk_test NO DELAY")

    try:
        wait_for_delete_files(0)
    finally:
        # Remove extra objects to prevent tests cascade failing
        for p in remote_disk_path.glob("*"):
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_PER_PART_WIDE = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6 + 1
FILES_OVERHEAD_PER_PART_COMPACT = 10 + 1


def wait_for_delete_files(expected, num_tries=30):
    while num_tries > 0:
        if files_number_in_disk() == expected:
            break
        num_tries -= 1
        time.sleep(1)
    assert files_number_in_disk() == expected



def wait_for_client(client):
    for _ in range(5):
        try:
            assert (client.query("SELECT 1") == "1\n")
        except QueryRuntimeException as e:
            if e.returncode != 209 and e.returncode != 210: # Timeout or Connection reset by peer
                raise
        time.sleep(15)



@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "server", main_configs=["configs/config.d/server_storage_conf.xml"]
        )
        cluster.add_instance(
            "client", main_configs=["configs/config.d/client_storage_conf.xml"]
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        wait_for_client(cluster.instances["client"])

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [(0, FILES_OVERHEAD_PER_PART_WIDE), (8192, FILES_OVERHEAD_PER_PART_COMPACT)],
)
def test_simple_insert_select(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        "remote_disk_test",
        additional_settings="min_rows_for_wide_part={}".format(min_rows_for_wide_part),
    )

    node = cluster.instances["client"]

    values1 = generate_values("2020-01-03", 4096)
    node.query("INSERT INTO remote_disk_test VALUES {}".format(values1))
    assert (
        node.query("SELECT * FROM remote_disk_test order by dt, id FORMAT Values") == values1
    )

    
    assert files_number_in_disk() == FILES_OVERHEAD + files_per_part

    values2 = generate_values("2020-01-04", 4096)
    node.query("INSERT INTO remote_disk_test VALUES {}".format(values2))
    assert (
        node.query("SELECT * FROM remote_disk_test ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )

    assert files_number_in_disk() == FILES_OVERHEAD + files_per_part * 2

    assert (
        node.query("SELECT count(*) FROM remote_disk_test where id = 1 FORMAT Values") == "(2)"
    )


def test_alter_table_columns(cluster):
    create_table(cluster, "remote_disk_test")

    node = cluster.instances["client"]

    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(
            generate_values("2020-01-03", 4096, -1)
        )
    )

    node.query("ALTER TABLE remote_disk_test ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have merged
    node.query("OPTIMIZE TABLE remote_disk_test")

    assert node.query("SELECT sum(col1) FROM remote_disk_test FORMAT Values") == "(8192)"
    assert (
        node.query("SELECT sum(col1) FROM remote_disk_test WHERE id > 0 FORMAT Values")
        == "(4096)"
    )
    wait_for_delete_files(
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN,
    )

    node.query(
        "ALTER TABLE remote_disk_test MODIFY COLUMN col1 String",
        settings={"mutations_sync": 2},
    )

    assert node.query("SELECT distinct(col1) FROM remote_disk_test FORMAT Values") == "('1')"
    # and file with mutation
    wait_for_delete_files(
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN + 1,
    )

    node.query("ALTER TABLE remote_disk_test DROP COLUMN col1", settings={"mutations_sync": 2})

    # and 2 files with mutations
    wait_for_delete_files(
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + 2
    )


def test_attach_detach_partition(cluster):
    create_table(cluster, "remote_disk_test")

    node = cluster.instances["client"]

    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE remote_disk_test DETACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "remote_disk_test")
    wait_for_delete_inactive_parts(node, "remote_disk_test")
    wait_for_delete_files(
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("ALTER TABLE remote_disk_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE remote_disk_test DROP PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "remote_disk_test")
    wait_for_delete_inactive_parts(node, "remote_disk_test")
    wait_for_delete_files(FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query("ALTER TABLE remote_disk_test DETACH PARTITION '2020-01-04'")
    node.query(
        "ALTER TABLE remote_disk_test DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "remote_disk_test")
    wait_for_delete_inactive_parts(node, "remote_disk_test")
    wait_for_delete_files(FILES_OVERHEAD)


def test_move_partition_to_another_disk(cluster):
    create_table(cluster, "remote_disk_test")

    node = cluster.instances["client"]

    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("ALTER TABLE remote_disk_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE

    node.query("ALTER TABLE remote_disk_test MOVE PARTITION '2020-01-04' TO DISK 'remote_disk'")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2


def test_table_manipulations(cluster):
    create_table(cluster, "remote_disk_test")

    node = cluster.instances["client"]

    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )

    node.query("RENAME TABLE remote_disk_test TO remote_disk_renamed")
    assert node.query("SELECT count(*) FROM remote_disk_renamed FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("RENAME TABLE remote_disk_renamed TO remote_disk_test")
    assert node.query("CHECK TABLE remote_disk_test FORMAT Values") == "(1)"

    node.query("DETACH TABLE remote_disk_test")
    node.query("ATTACH TABLE remote_disk_test")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2

    node.query("TRUNCATE TABLE remote_disk_test")
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "remote_disk_test")
    wait_for_delete_inactive_parts(node, "remote_disk_test")
    wait_for_delete_files(FILES_OVERHEAD)


def test_move_replace_partition_to_another_table(cluster):
    create_table(cluster, "remote_disk_test")

    node = cluster.instances["client"]

    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(
            generate_values("2020-01-05", 4096, -1)
        )
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(
            generate_values("2020-01-06", 4096, -1)
        )
    )
    assert node.query("SELECT sum(id) FROM remote_disk_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(16384)"

    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4

    create_table(cluster, "remote_disk_clone")

    node.query("ALTER TABLE remote_disk_test MOVE PARTITION '2020-01-03' TO TABLE remote_disk_clone")
    node.query("ALTER TABLE remote_disk_test MOVE PARTITION '2020-01-05' TO TABLE remote_disk_clone")
    assert node.query("SELECT sum(id) FROM remote_disk_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(id) FROM remote_disk_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_clone FORMAT Values") == "(8192)"

    # Number of objects on remote disk should be unchanged.
    assert files_number_in_disk() == FILES_OVERHEAD * 2 + FILES_OVERHEAD_PER_PART_WIDE * 4

    # Add new partitions to source table, but with different values and replace them from copied table.
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(
            generate_values("2020-01-03", 4096, -1)
        )
    )
    node.query(
        "INSERT INTO remote_disk_test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    assert node.query("SELECT sum(id) FROM remote_disk_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(16384)"

    assert files_number_in_disk() == FILES_OVERHEAD * 2 + FILES_OVERHEAD_PER_PART_WIDE * 6

    node.query("ALTER TABLE remote_disk_test REPLACE PARTITION '2020-01-03' FROM remote_disk_clone")
    node.query("ALTER TABLE remote_disk_test REPLACE PARTITION '2020-01-05' FROM remote_disk_clone")
    assert node.query("SELECT sum(id) FROM remote_disk_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(16384)"
    assert node.query("SELECT sum(id) FROM remote_disk_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_clone FORMAT Values") == "(8192)"

    # Wait for outdated partitions deletion.
    wait_for_delete_files(
        FILES_OVERHEAD * 2 + FILES_OVERHEAD_PER_PART_WIDE * 6 # TODO find out why in hdfs and s3 tests there is 4 instead of 6
    )

    node.query("DROP TABLE remote_disk_clone NO DELAY")
    assert node.query("SELECT sum(id) FROM remote_disk_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM remote_disk_test FORMAT Values") == "(16384)"

    # Data should remain on remote disk
    assert files_number_in_disk() == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4