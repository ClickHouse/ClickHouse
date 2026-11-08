import logging
import os
import time

import pytest
from pyhdfs import HdfsClient

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.config_manager import ConfigManager
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
            temporary_directories_lifetime=1,
            write_marks_for_substreams_in_compact_parts=1,
            cleanup_delay_period=1,
            cleanup_delay_period_random_add=0,
            cleanup_thread_preferred_points_per_iteration=0,
            auto_statistics_types=''
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
FILES_OVERHEAD_COLUMNS_SUBSTREAMS = 1
# The minmax skip index goes into a single skp_idx.packed archive instead of a
# separate .idx2 + .mrk2 pair, because packed_skip_index_max_bytes is 1 MiB by default.
FILES_SAVED_BY_PACKED_SKIP_INDEX = 1
FILES_OVERHEAD_PER_PART_WIDE = (
    FILES_OVERHEAD_PER_COLUMN * 3
    + 2
    + 6
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
    + FILES_OVERHEAD_COLUMNS_SUBSTREAMS
    - FILES_SAVED_BY_PACKED_SKIP_INDEX
)
FILES_OVERHEAD_PER_PART_COMPACT = (
    10
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
    + FILES_OVERHEAD_COLUMNS_SUBSTREAMS
    - FILES_SAVED_BY_PACKED_SKIP_INDEX
)
FILES_OVERHEAD_PER_INVALIDATED_COLUMN = 1


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/hdfs_locations.xml",
            ],
            macros={"hdfs_host": "hdfs1"},
            with_hdfs=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")
        fs.mkdirs("/clickhouse")

        logging.info("Created HDFS directory")

        yield cluster
    finally:
        cluster.shutdown()


def list_hdfs_objects(fs, path="/clickhouse"):
    # The object keys contain a nested directory prefix (e.g. `abc/xyz...`),
    # so list the files recursively; directories are not objects.
    return [
        root + "/" + name for root, _, files in fs.walk(path) for name in files
    ]


def count_hdfs_objects(fs, path="/clickhouse"):
    # `fileCount` is recursive: it counts the files of the whole subtree in one request.
    return fs.get_content_summary(path).fileCount


def assert_hdfs_objects_count(fs, expected):
    count = count_hdfs_objects(fs)
    assert count == expected, f"{count} != {expected}: {list_hdfs_objects(fs)}"


def wait_for_delete_hdfs_objects(cluster, expected, num_tries=30):
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")
    while num_tries > 0:
        num_hdfs_objects = count_hdfs_objects(fs)
        if num_hdfs_objects == expected:
            break
        num_tries -= 1
        time.sleep(1)
    assert_hdfs_objects_count(fs, expected)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    node = cluster.instances["node"]

    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")
    print("Number of hdfs objects to delete:", count_hdfs_objects(fs), sep=" ")

    node.query("DROP TABLE IF EXISTS hdfs_test SYNC")

    try:
        wait_for_delete_hdfs_objects(cluster, 0)
    finally:
        if count_hdfs_objects(fs) == 0:
            return
        hdfs_objects = list_hdfs_objects(fs)
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

    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + files_per_part)

    values2 = generate_values("2020-01-04", 4096)
    node.query("INSERT INTO hdfs_test VALUES {}".format(values2))
    assert (
        node.query("SELECT * FROM hdfs_test ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + files_per_part * 2)

    assert (
        node.query("SELECT count(*) FROM hdfs_test where id = 1 FORMAT Values") == "(2)"
    )


def test_alter_table_columns(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]

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
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("ALTER TABLE hdfs_test DETACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(
        cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("ALTER TABLE hdfs_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN,
    )

    node.query("ALTER TABLE hdfs_test DROP PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(4096)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query("ALTER TABLE hdfs_test DETACH PARTITION '2020-01-04'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query(
        "ALTER TABLE hdfs_test DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(0)"
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD)


def test_move_partition_to_another_disk(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-04' TO DISK 'hdfs'")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)


def test_table_manipulations(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")

    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO hdfs_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )

    node.query("RENAME TABLE hdfs_test TO hdfs_renamed")
    assert node.query("SELECT count(*) FROM hdfs_renamed FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("RENAME TABLE hdfs_renamed TO hdfs_test")
    assert (
        node.query(
            "CHECK TABLE hdfs_test FORMAT Values SETTINGS check_query_single_value_result = 1"
        )
        == "(1)"
    )

    node.query("DETACH TABLE hdfs_test")
    node.query("ATTACH TABLE hdfs_test")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("TRUNCATE TABLE hdfs_test")
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(0)"
    wait_for_delete_empty_parts(node, "hdfs_test")
    wait_for_delete_inactive_parts(node, "hdfs_test")
    wait_for_delete_hdfs_objects(cluster, FILES_OVERHEAD)


def test_move_replace_partition_to_another_table(cluster):
    create_table(cluster, "hdfs_test")

    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")

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

    assert_hdfs_objects_count(fs, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4)

    create_table(cluster, "hdfs_clone")

    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-03' TO TABLE hdfs_clone")
    node.query("ALTER TABLE hdfs_test MOVE PARTITION '2020-01-05' TO TABLE hdfs_clone")
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(id) FROM hdfs_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_clone FORMAT Values") == "(8192)"

    # Number of objects in HDFS should be unchanged.
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
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

    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 6
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
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
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 4,
    )

    node.query("DROP TABLE hdfs_clone SYNC")
    assert node.query("SELECT sum(id) FROM hdfs_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM hdfs_test FORMAT Values") == "(16384)"

    # Data should remain in hdfs
    wait_for_delete_hdfs_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
    )


def test_sql_hdfs_disk_remote_host_filter(cluster):
    node = cluster.instances["node"]
    fs = HdfsClient(hosts=cluster.hdfs_ip, user_name="root")
    include_settings = {
        "dynamic_disk_allow_include": 1,
        "s3_allow_server_credentials_in_user_queries": 1,
    }

    def hdfs_disk(path, host="hdfs1"):
        return f"disk(type = 'hdfs', endpoint = 'hdfs://{host}:9000/sql_disk_filter/{path}/')"

    existing_disk = hdfs_disk("filter_existing")
    macro_disk = hdfs_disk("filter_macro", host="{hdfs_host}")
    locations_disk = "disk(type = 'object_storage', include = 'hdfs_locations')"
    named_disk = (
        "disk(name = 'old_hdfs', type = 'hdfs', "
        "endpoint = 'hdfs://hdfs1:9000/sql_disk_filter/filter_named/')"
    )
    read_only_disk = (
        "disk(type = 'hdfs', endpoint = 'hdfs://hdfs1:9000/sql_disk_filter/filter_ro/', read_only = 1)"
    )
    include_wrapper_disk = "disk(type = 'encrypted', include = 'encrypted_old_hdfs')"
    include_config = "/etc/clickhouse-server/config.d/hdfs_locations.xml"
    wrapper_disks = [
        "disk(type = 'encrypted', disk = 'old_hdfs', key = '0123456789abcdef', path = 'bypass/')",
        "disk(type = 'cache', disk = 'old_hdfs', path = 'hdfs_bypass_cache/', max_size = '1Mi')",
    ]

    def create(table, disk, settings=None):
        node.query(
            f"CREATE TABLE {table} (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS disk = {disk}",
            settings=settings,
        )
        node.query(f"INSERT INTO {table} VALUES (1)")

    def assert_rejected(disk, settings=None):
        error = node.query_and_get_error(
            f"CREATE TABLE t_rejected_hdfs_disk (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS disk = {disk}",
            settings=settings,
        )
        assert "UNACCEPTABLE_URL" in error, error

    def assert_reuse_rejected():
        assert_rejected(existing_disk)
        assert_rejected(locations_disk, settings=include_settings)
        # A new wrapper takes the old custom disk by name, without building an HDFS object storage.
        for wrapper in wrapper_disks:
            assert_rejected(wrapper)

    # Created before the list excludes hdfs1: loaded from metadata on restart, so they are only reported.
    create("t_existing_hdfs_disk", existing_disk)
    create("t_locations_hdfs_disk", locations_disk, settings=include_settings)
    create("t_named_hdfs_disk", named_disk)
    create("t_macro_hdfs_disk", macro_disk)
    create("t_include_wrapper_disk", include_wrapper_disk, settings=include_settings)
    # The disk is cached when it is created, even if a MergeTree table on an empty read-only disk then fails.
    node.query_and_get_answer_with_error(
        f"CREATE TABLE t_ro_hdfs_disk (x UInt32) ENGINE = MergeTree ORDER BY x SETTINGS disk = {read_only_disk}"
    )

    with ConfigManager() as cm:
        cm.add_main_config(node, "configs/remote_url_allow_hosts.xml")

        assert_rejected(hdfs_disk("filter_blocked"))
        # Rejected before HDFSObjectStorage connects, so the access check wrote nothing.
        assert not fs.exists("/sql_disk_filter/filter_blocked")
        # Identical definitions reuse the disks created before the list was tightened.
        assert_reuse_rejected()
        assert_rejected(read_only_disk)

        # The cached wrapper still delegates to old_hdfs even though its include now names a server disk.
        node.replace_in_config(include_config, "<disk>old_hdfs</disk>", "<disk>default</disk>")
        try:
            assert_rejected(include_wrapper_disk, settings=include_settings)
        finally:
            node.replace_in_config(include_config, "<disk>default</disk>", "<disk>old_hdfs</disk>")
        # Its disk wraps the disk of another table, which may not be loaded yet on restart.
        node.query("DROP TABLE t_include_wrapper_disk SYNC")

        # The macro now expands to an allowed host, but the reused disk still points at hdfs1.
        cm.add_main_config(node, "configs/macros_override.xml")
        assert node.query("SELECT getMacro('hdfs_host')") == "allowed.example.com\n"
        assert_rejected(macro_disk)
        # On restart the macro would point the table at a nonexistent host.
        node.query("DROP TABLE t_macro_hdfs_disk SYNC")

        node.restart_clickhouse()
        assert node.query("SELECT x FROM t_existing_hdfs_disk") == "1\n"
        assert node.query("SELECT x FROM t_locations_hdfs_disk") == "1\n"
        assert node.query("SELECT x FROM t_named_hdfs_disk") == "1\n"
        assert node.contains_in_log("is not allowed by `remote_url_allow_hosts`")
        # Identical definitions reuse the disks loaded from metadata with only a warning.
        assert_reuse_rejected()

        # A short ATTACH reuses the disk of its stored definition, so it is not checked.
        node.query("DETACH TABLE t_existing_hdfs_disk")
        node.query("ATTACH TABLE t_existing_hdfs_disk")
        assert node.query("SELECT x FROM t_existing_hdfs_disk") == "1\n"

    for table in ["t_existing_hdfs_disk", "t_locations_hdfs_disk", "t_named_hdfs_disk"]:
        node.query(f"DROP TABLE {table} SYNC")
    node.query("DROP TABLE IF EXISTS t_ro_hdfs_disk SYNC")
