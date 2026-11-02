# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def break_part(data_path, part_name):

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"rm {data_path}/{part_name}/primary.cidx",
        ]
    )


def remove_part(data_path, part_name):
    node.exec_in_container(["bash", "-c", f"rm -r {data_path}/{part_name}"])


def get_count(table):
    return int(node.query(f"SELECT count() FROM {table}").strip())


def detach_table(table):
    node.query(f"DETACH TABLE {table}")


def attach_table(table):
    node.query(f"ATTACH TABLE {table}")


def check_table(table):
    rows = 900
    per_part_rows = 90

    node.query(f"INSERT INTO {table} SELECT * FROM numbers(900)")

    assert get_count(table) == rows

    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='{table}'"
    ).strip()

    # break one part, and check that clickhouse will be alive
    break_part(data_path, "0_1_1_0")
    rows -= per_part_rows
    detach_table(table)
    attach_table(table)
    assert get_count(table) == rows

    # break two parts, and check that clickhouse will not start
    break_part(data_path, "1_2_2_0")
    break_part(data_path, "2_3_3_0")
    rows -= per_part_rows * 2
    detach_table(table)
    with pytest.raises(QueryRuntimeException):
        attach_table(table)

    # now remove one part, and check
    remove_part(data_path, "1_2_2_0")
    attach_table(table)
    assert get_count(table) == rows

    node.query(f"DROP TABLE {table}")


def test_max_suspicious_broken_parts():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        max_suspicious_broken_parts = 1;
    """
    )
    check_table("test_max_suspicious_broken_parts")


def test_max_suspicious_broken_parts_bytes():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts_bytes (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        max_suspicious_broken_parts = 10,
        serialization_info_version = 'basic',
        /* one part takes ~1010 byte, so we allow failure of one part with these limit */
        max_suspicious_broken_parts_bytes = 1500,
        /* Disable implicit index and statistics as they'd change the size */
        add_minmax_index_for_numeric_columns=0,
        auto_statistics_types='';
    """
    )
    check_table("test_max_suspicious_broken_parts_bytes")


def test_max_suspicious_broken_parts__wide():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts__wide (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        min_bytes_for_wide_part = 0,
        max_suspicious_broken_parts = 1;
    """
    )
    check_table("test_max_suspicious_broken_parts__wide")


def test_max_suspicious_broken_parts_bytes__wide():
    node.query(
        """
    CREATE TABLE test_max_suspicious_broken_parts_bytes__wide (
        key Int
    )
    ENGINE=MergeTree
    ORDER BY key
    PARTITION BY key%10
    SETTINGS
        min_bytes_for_wide_part = 0,
        serialization_info_version = 'basic',
        max_suspicious_broken_parts = 10,
        /* one part takes ~1009 byte, so we allow failure of one part with these limit */
        max_suspicious_broken_parts_bytes = 1500,
        /* Disable implicit index and statistics as they'd change the size */
        add_minmax_index_for_numeric_columns=0,
        auto_statistics_types='';
    """
    )
    check_table("test_max_suspicious_broken_parts_bytes__wide")


def bash(cmd):
    return node.exec_in_container(["bash", "-c", cmd], privileged=True, user="root")


def create_table_with_patch_part(table, engine):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
    CREATE TABLE {table} (id UInt64, v Int64)
    ENGINE = {engine}
    ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
    """
    )
    node.query(f"INSERT INTO {table} SELECT number, 0 FROM numbers(10)")
    node.query(f"UPDATE {table} SET v = 1 WHERE id = 3")
    assert node.query(f"SELECT sum(v) FROM {table}") == "1\n"

    rows = (
        node.query(
            f"SELECT name, path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active AND startsWith(partition_id, 'patch-')"
        )
        .strip()
        .splitlines()
    )
    assert len(rows) == 1, rows
    patch_name, patch_path = rows[0].split("\t")
    assert patch_path.startswith("/"), patch_path
    return patch_name, patch_path


def write_first_byte(path, byte_hex):
    bash(f"printf '\\x{byte_hex}' | dd of={path} bs=1 count=1 conv=notrunc")


def test_patch_part_in_unsupported_format_version_is_kept():
    table = "t_unsupported_patch"
    patch_name, patch_path = create_table_with_patch_part(table, "MergeTree")

    index_file = f"{patch_path}source_parts.dat"
    original_byte = bash(f"od -An -tx1 -N1 {index_file}").strip()
    size = bash(f"stat -c%s {index_file}").strip()

    node.query(f"DETACH TABLE {table}")
    write_first_byte(index_file, "7f")
    assert bash(f"stat -c%s {index_file}").strip() == size

    error = node.query_and_get_error(f"ATTACH TABLE {table}")
    assert "UNKNOWN_FORMAT_VERSION" in error, error
    assert patch_name in error, error
    patch_dir = patch_path.rstrip("/")
    assert node.contains_in_log(
        f"{patch_dir} (size: .*) has a format version that this server does not support"
    )
    assert not node.contains_in_log(f"Detaching broken part {patch_dir}")
    assert not node.contains_in_log(f"Part {patch_path} is broken")

    table_path = patch_path.rstrip("/").rsplit("/", 1)[0]
    assert bash(f"test -d {patch_path} && echo yes || echo no").strip() == "yes"
    assert "broken-on-start" not in bash(
        f"ls -A {table_path}/detached/ 2>/dev/null || true"
    )

    write_first_byte(index_file, original_byte)
    node.query(f"ATTACH TABLE {table}")
    assert node.query(f"SELECT sum(v) FROM {table}") == "1\n"
    node.query(f"DROP TABLE {table} SYNC")


def test_part_with_unsupported_serialization_version_is_kept():
    table = "t_unsupported_serialization"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
    CREATE TABLE {table} (id UInt64, s String)
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS serialization_info_version = 'with_types'
    """
    )
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(10)")

    rows = (
        node.query(
            f"SELECT name, path FROM system.parts WHERE database = 'default' AND table = '{table}' AND active"
        )
        .strip()
        .splitlines()
    )
    assert len(rows) == 1, rows
    part_name, part_path = rows[0].split("\t")
    table_path = part_path.rstrip("/").rsplit("/", 1)[0]

    serialization_file = f"{part_path}serialization.json"
    assert bash(f"test -f {serialization_file} && echo yes || echo no").strip() == "yes"
    size = bash(f"stat -c%s {serialization_file}").strip()

    node.query(f"DETACH TABLE {table}")
    # The top-level version is the last key of the file: `,"version":N}`.
    original_version = bash(f"tail -c 2 {serialization_file} | head -c 1")
    assert original_version.isdigit(), original_version
    bash('sed -i \'s/"version":[0-9]}$/"version":9}/\' ' + serialization_file)
    assert bash(f"stat -c%s {serialization_file}").strip() == size
    assert bash(f"tail -c 12 {serialization_file}") == '"version":9}'

    error = node.query_and_get_error(f"ATTACH TABLE {table}")
    assert "UNKNOWN_FORMAT_VERSION" in error, error
    assert part_name in error, error
    assert bash(f"test -d {part_path} && echo yes || echo no").strip() == "yes"
    assert "broken-on-start" not in bash(
        f"ls -A {table_path}/detached/ 2>/dev/null || true"
    )

    bash(
        f'sed -i \'s/"version":9}}$/"version":{original_version}}}/\' '
        + serialization_file
    )
    node.query(f"ATTACH TABLE {table}")
    assert get_count(table) == 10
    node.query(f"DROP TABLE {table} SYNC")


def test_force_restore_data_detaches_part_in_unsupported_format_version():
    table = "t_unsupported_force"
    patch_name, patch_path = create_table_with_patch_part(table, "MergeTree")

    index_file = f"{patch_path}source_parts.dat"
    size = bash(f"stat -c%s {index_file}").strip()

    node.query(f"DETACH TABLE {table}")
    write_first_byte(index_file, "7f")
    assert bash(f"stat -c%s {index_file}").strip() == size
    bash(
        "mkdir -p /var/lib/clickhouse/flags && touch /var/lib/clickhouse/flags/force_restore_data && (chown -R clickhouse:clickhouse /var/lib/clickhouse/flags 2>/dev/null || true)"
    )
    node.restart_clickhouse()

    assert (
        bash(
            "test -e /var/lib/clickhouse/flags/force_restore_data && echo yes || echo no"
        ).strip()
        == "no"
    )
    assert node.query(f"SELECT sum(v) FROM {table}") == "0\n"
    assert (
        node.query(
            f"SELECT name, reason FROM system.detached_parts WHERE database = 'default' AND table = '{table}'"
        )
        == f"broken-on-start_{patch_name}\tbroken-on-start\n"
    )
    node.query(f"DROP TABLE {table} SYNC")
