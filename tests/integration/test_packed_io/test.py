import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1", main_configs=["configs/storage_conf.xml"], with_minio=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_part_path(table_name, part_name):
    return node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table_name, part_name
        )
    ).strip()


def get_relative_part_path(table_name, part_name):
    disk_name = node.query(
        "SELECT disk_name FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table_name, part_name
        )
    ).strip()

    disk_path = node.query(
        "SELECT path FROM system.disks WHERE name = '{}'".format(disk_name)
    ).strip()

    part_path = get_part_path(table_name, part_name)
    return os.path.relpath(part_path, disk_path)


def check_diff(lhs, rhs):
    node.exec_in_container(
        ["bash", "-c", f"diff -r {lhs} {rhs}"], privileged=True, user="root"
    )


def list_files_in_part(part_path):
    data_path = os.path.join(part_path, "data.packed")

    return node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks --config /etc/clickhouse-server/config.xml --disk s3 --query \"packed-io list {data_path}\" | awk '{{print $1}}'",
        ],
        privileged=True,
        user="root",
    )


def list_files_in_part_recursive(part_path):
    return node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks --config /etc/clickhouse-server/config.xml --disk s3 --query \"packed-io --recursive list {part_path}\" | awk '{{print $1}}'",
        ],
        privileged=True,
        user="root",
    )


def unpack_part(part_path, output_dir):
    data_path = os.path.join(part_path, "data.packed")

    node.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse disks --config /etc/clickhouse-server/config.xml --disk local --query "packed-io extract --disk-from s3 {data_path} {output_dir}"',
        ],
        privileged=True,
        user="root",
    )


def unpack_part_recursive(part_path, output_dir):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse disks --config /etc/clickhouse-server/config.xml --disk local --query "packed-io --recursive extract --disk-from s3 {part_path} {output_dir}"',
        ],
        privileged=True,
        user="root",
    )


def pack_part(part_path, output_dir, file_order_hint):
    output_file = os.path.join(output_dir, "data.packed")

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks --config /etc/clickhouse-server/config.xml --disk local --query \"packed-io create --disk-from s3 {part_path} {output_file} --file-order '{file_order_hint}'\"",
        ],
        privileged=True,
        user="root",
    )


def pack_part_recursive(part_path, output_dir, file_order_hint):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse disks --config /etc/clickhouse-server/config.xml --disk local --query \"packed-io --recursive create --disk-from s3 {part_path} {output_dir} --file-order '{file_order_hint}'\"",
        ],
        privileged=True,
        user="root",
    )


def test_packed_io(started_cluster):
    node.query(
        "CREATE OR REPLACE TABLE t_packed_local (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '10M'"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_full_local (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = 0"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_full_s3 (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '0', storage_policy = 's3'"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_packed_s3 (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '10M', storage_policy = 's3'"
    )
    node.query(
        "INSERT INTO t_full_local SELECT number, randomPrintableASCII(10) FROM numbers(100)"
    )

    node.query("INSERT INTO t_full_s3 SELECT * FROM t_full_local")
    node.query("INSERT INTO t_packed_local SELECT * FROM t_full_local")
    node.query("INSERT INTO t_packed_s3 SELECT * FROM t_full_local")

    part_path_s3 = get_relative_part_path("t_packed_s3", "all_1_1_0")
    part_path_local = get_part_path("t_full_local", "all_1_1_0")

    files_in_part = list_files_in_part(part_path_s3)
    print(files_in_part)

    unpack_part(part_path_s3, "./unpacked_0")
    check_diff("./unpacked_0", part_path_local)

    unpack_part_recursive(part_path_s3, "./unpacked_1")
    check_diff("./unpacked_1/all_1_1_0", part_path_local)

    part_path_s3 = get_relative_part_path("t_full_s3", "all_1_1_0")
    part_path_local = get_part_path("t_packed_local", "all_1_1_0")

    pack_part(part_path_s3, "./packed0", files_in_part)
    check_diff("./packed0", part_path_local)

    pack_part_recursive(part_path_s3, "./packed1", files_in_part)
    check_diff("./packed1/all_1_1_0", part_path_local)


def test_packed_io_projections(started_cluster):
    node.query(
        "CREATE OR REPLACE TABLE t_packed_local_proj (id UInt64, s String, PROJECTION p (SELECT sum(id), max(s))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '10M'"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_full_local_proj (id UInt64, s String, PROJECTION p (SELECT sum(id), max(s))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = 0"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_full_s3_proj (id UInt64, s String, PROJECTION p (SELECT sum(id), max(s))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '0', storage_policy = 's3'"
    )
    node.query(
        "CREATE OR REPLACE TABLE t_packed_s3_proj (id UInt64, s String, PROJECTION p (SELECT sum(id), max(s))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '10M', storage_policy = 's3'"
    )
    node.query(
        "INSERT INTO t_full_local_proj SELECT number, randomPrintableASCII(10) FROM numbers(100)"
    )

    node.query("INSERT INTO t_full_s3_proj SELECT * FROM t_full_local_proj")
    node.query("INSERT INTO t_packed_local_proj SELECT * FROM t_full_local_proj")
    node.query("INSERT INTO t_packed_s3_proj SELECT * FROM t_full_local_proj")

    part_path_s3 = get_relative_part_path("t_packed_s3_proj", "all_1_1_0")
    proj_path_s3 = os.path.join(part_path_s3, "p.proj")

    part_path_local = get_part_path("t_full_local_proj", "all_1_1_0")
    proj_path_local = os.path.join(part_path_local, "p.proj")

    files_in_part = list_files_in_part_recursive(part_path_s3)
    print(files_in_part)

    files_in_proj = list_files_in_part_recursive(proj_path_s3)
    print(files_in_proj)

    unpack_part(proj_path_s3, "./unpacked_2")
    check_diff("./unpacked_2", proj_path_local)

    unpack_part_recursive(part_path_s3, "./unpacked_3")
    check_diff("./unpacked_3/all_1_1_0", part_path_local)
    check_diff("./unpacked_3/all_1_1_0/p.proj", proj_path_local)

    part_path_s3 = get_relative_part_path("t_full_s3_proj", "all_1_1_0")
    proj_path_s3 = os.path.join(part_path_s3, "p.proj")

    part_path_local = get_part_path("t_packed_local_proj", "all_1_1_0")
    proj_path_local = os.path.join(part_path_local, "p.proj")

    pack_part(proj_path_s3, "./packed_2", files_in_proj)
    check_diff("./packed_2", proj_path_local)

    pack_part_recursive(part_path_s3, "./packed_3", files_in_part)
    check_diff("./packed_3/all_1_1_0", part_path_local)
    check_diff("./packed_3/all_1_1_0/p.proj", proj_path_local)



def check_compact_part_proj_file_order(proj_files_in_part):
    assert(proj_files_in_part[0] == "checksums.txt")
    assert(proj_files_in_part[1] == "columns.txt")
    assert(proj_files_in_part[2] == "columns_substreams.txt")
    assert(proj_files_in_part[3] == "count.txt")
    assert(proj_files_in_part[4] == "metadata_version.txt")
    assert(proj_files_in_part[5] == "default_compression_codec.txt")
    assert(proj_files_in_part[6] == "serialization.json")
    assert(proj_files_in_part[7] == "data.cmrk4")
    assert(proj_files_in_part[8] == "primary.cidx") # primary index
def check_wide_part_proj_file_order(proj_files_in_part):
    assert(proj_files_in_part[0] == "checksums.txt")
    assert(proj_files_in_part[1] == "columns.txt")
    assert(proj_files_in_part[2] == "columns_substreams.txt")
    assert(proj_files_in_part[3] == "count.txt")
    assert(proj_files_in_part[4] == "metadata_version.txt")
    assert(proj_files_in_part[5] == "default_compression_codec.txt")
    assert(proj_files_in_part[6] == "serialization.json")
    assert(proj_files_in_part[7] == "primary.cidx") # primary index
    assert(proj_files_in_part[8] == "s1.cmrk2")     # first column that is used for projection loadIndexGranularity
    assert(proj_files_in_part[9] == "max%28k2%29.cmrk2")
    assert(proj_files_in_part[10] == "sum%28v1%29.cmrk2")

def test_packed_io_file_order(started_cluster):
    # A table with many different features to test various files in the part
    node.query(
        """
        CREATE OR REPLACE TABLE t_packed_s3_file_order
        (
            k1 UInt64,
            k2 Decimal(32,9),
            k3 String,
            s1 String,
            s2 Int32,
            v1 Int64 CODEC(Delta, ZSTD(3)),
            v2 String,
            v1_alias ALIAS v1,
            v3 DateTime('UTC') DEFAULT '2024-08-24',
            v4 UInt64 DEFAULT 42,
            PRIMARY KEY (k1 + k2,k3),
            INDEX idx_s2_min_max s2+v1_alias-k1 TYPE minmax GRANULARITY 2,
            INDEX idx_v2_bf v2||k3 TYPE bloom_filter(0.01) GRANULARITY 3,
            PROJECTION proj_sum_by_s1
            (
                SELECT max(k2), sum(v1) GROUP BY s1
            ),
            PROJECTION proj_avg_by_v2
            (
                SELECT avg(k1), sum(s2) GROUP BY v2
            )
        )
        ENGINE = MergeTree()
        PARTITION BY (toStartOfDay(v3 + v4))
        ORDER BY (k1 + k2,k3,s1 || ' : ' || s2::String)
        TTL v3 + INTERVAL 30 YEAR
        SETTINGS
            min_rows_for_wide_part = 1,
            min_bytes_for_wide_part = 1,
            min_rows_for_full_part_storage = '100M',
            min_bytes_for_full_part_storage = '100M',
            assign_part_uuids = 1,
            storage_policy = 's3'
        """
    )

    node.query(
        "INSERT INTO t_packed_s3_file_order SELECT number, 12, 'k3_1', 's1_1', 1, 1, 'v2_1', '2024-09-24 16:00:00', 43 FROM numbers(1_000)"
    )

    part_path_s3 = get_relative_part_path("t_packed_s3_file_order", "1727136000_1_1_0")
    files_in_part = list_files_in_part(part_path_s3).strip().split("\n")
    assert(files_in_part[0] == "uuid.txt")
    assert(files_in_part[1] == "checksums.txt")
    assert(files_in_part[2] == "columns.txt")
    assert(files_in_part[3] == "columns_substreams.txt")
    assert(files_in_part[4] == "count.txt")
    assert(files_in_part[5] == "metadata_version.txt")
    assert(files_in_part[6] == "default_compression_codec.txt")
    assert(files_in_part[7] == "serialization.json")
    assert(files_in_part[8] == "partition.dat")
    assert(files_in_part[9] == "ttl.txt")
    assert(files_in_part[10] == "minmax_v3.idx") # minmax for partition key column v3
    assert(files_in_part[11] == "minmax_v4.idx") # minmax for partition key column v4
    assert(files_in_part[12] == "k1.cmrk2")     # first column that is used for loadIndexGranularity
    assert(files_in_part[13] == "primary.cidx") # primary index
    assert(files_in_part[14] == "k2.cmrk2")
    assert(files_in_part[15] == "k3.cmrk2")
    # Rest of the files are not that important to check
    # TODO: Maybe skip index files should be moved before data files

    #Test projection
    proj_path_s3 = os.path.join(part_path_s3, "proj_sum_by_s1.proj")
    proj_files_in_part = list_files_in_part(proj_path_s3).strip().split("\n")
    check_wide_part_proj_file_order(proj_files_in_part)

    #Test merged projection
    node.query(
        "INSERT INTO t_packed_s3_file_order SELECT number, 13, 'k3_1', 's1_1', 1, 1, 'v2_1', '2024-09-24 16:00:00', 43 FROM numbers(1_000)"
    )
    node.query(
        "OPTIMIZE TABLE t_packed_s3_file_order FINAL"
    )

    part_path_s3 = get_relative_part_path("t_packed_s3_file_order", "1727136000_1_2_1")
    proj_path_s3 = os.path.join(part_path_s3, "proj_sum_by_s1.proj")
    proj_files_in_part = list_files_in_part(proj_path_s3).strip().split("\n")
    # check_wide_part_proj_file_order(proj_files_in_part)
    assert(proj_files_in_part[0] == "checksums.txt")
    assert(proj_files_in_part[1] == "columns.txt")
    assert(proj_files_in_part[2] == "columns_substreams.txt")
    assert(proj_files_in_part[3] == "count.txt")
    assert(proj_files_in_part[4] == "metadata_version.txt")
    assert(proj_files_in_part[5] == "default_compression_codec.txt")
    assert(proj_files_in_part[6] == "serialization.json")
    #TODO need to figure out why s1.cmrk2 and primary.cidx switch order after merge
    assert(proj_files_in_part[7] == "s1.cmrk2")
    assert(proj_files_in_part[8] == "primary.cidx")
    assert(proj_files_in_part[9] == "max%28k2%29.cmrk2")
    assert(proj_files_in_part[10] == "sum%28v1%29.cmrk2")


def test_packed_io_compact_file_order(started_cluster):
    # A table with many different features to test various files in the part
    node.query(
        """
        CREATE OR REPLACE TABLE t_packed_compact_s3_file_order
        (
            k1 UInt64,
            k2 Decimal(32,9),
            k3 String,
            s1 String,
            s2 Int32,
            v1 Int64 CODEC(Delta, ZSTD(3)),
            v2 String,
            v1_alias ALIAS v1,
            v3 DateTime('UTC') DEFAULT '2024-08-24',
            v4 UInt64 DEFAULT 42,
            PRIMARY KEY (k1 + k2,k3),
            INDEX idx_s2_min_max s2+v1_alias-k1 TYPE minmax GRANULARITY 2,
            INDEX idx_v2_bf v2||k3 TYPE bloom_filter(0.01) GRANULARITY 3,
            PROJECTION proj_sum_by_s1
            (
                SELECT max(k2), sum(v1) GROUP BY s1
            ),
            PROJECTION proj_avg_by_v2
            (
                SELECT avg(k1), sum(s2) GROUP BY v2
            )
        )
        ENGINE = MergeTree()
        PARTITION BY (toStartOfDay(v3 + v4))
        ORDER BY (k1 + k2,k3,s1 || ' : ' || s2::String)
        TTL v3 + INTERVAL 30 YEAR
        SETTINGS
            min_rows_for_wide_part = 100000,
            min_bytes_for_wide_part = 100000,
            min_rows_for_full_part_storage = '100M',
            min_bytes_for_full_part_storage = '100M',
            assign_part_uuids = 1,
            storage_policy = 's3',
            write_marks_for_substreams_in_compact_parts = 1
        """
    )

    node.query(
        "INSERT INTO t_packed_compact_s3_file_order SELECT number, 12, 'k3_1', 's1_1', 1, 1, 'v2_1', '2024-09-24 16:00:00', 43 FROM numbers(1_000)"
    )

    part_path_s3 = get_relative_part_path("t_packed_compact_s3_file_order", "1727136000_1_1_0")
    files_in_part = list_files_in_part(part_path_s3).strip().split("\n")
    assert(files_in_part[0] == "uuid.txt")
    assert(files_in_part[1] == "checksums.txt")
    assert(files_in_part[2] == "columns.txt")
    assert(files_in_part[3] == "columns_substreams.txt")
    assert(files_in_part[4] == "count.txt")
    assert(files_in_part[5] == "metadata_version.txt")
    assert(files_in_part[6] == "default_compression_codec.txt")
    assert(files_in_part[7] == "serialization.json")
    assert(files_in_part[8] == "partition.dat")
    assert(files_in_part[9] == "ttl.txt")
    assert(files_in_part[10] == "minmax_v3.idx") # minmax for partition key column v3
    assert(files_in_part[11] == "minmax_v4.idx") # minmax for partition key column v4
    assert(files_in_part[12] == "data.cmrk4")     # first column that is used for loadIndexGranularity
    assert(files_in_part[13] == "primary.cidx") # primary index

    #Test projection
    proj_path_s3 = os.path.join(part_path_s3, "proj_sum_by_s1.proj")
    proj_files_in_part = list_files_in_part(proj_path_s3).strip().split("\n")
    check_compact_part_proj_file_order(proj_files_in_part)

    #Test merged projection
    node.query(
        "INSERT INTO t_packed_compact_s3_file_order SELECT number, 13, 'k3_1', 's1_1', 1, 1, 'v2_1', '2024-09-24 16:00:00', 43 FROM numbers(1_000)"
    )
    node.query(
        "OPTIMIZE TABLE t_packed_compact_s3_file_order FINAL"
    )

    part_path_s3 = get_relative_part_path("t_packed_compact_s3_file_order", "1727136000_1_2_1")
    proj_path_s3 = os.path.join(part_path_s3, "proj_sum_by_s1.proj")
    proj_files_in_part = list_files_in_part(proj_path_s3).strip().split("\n")
    check_compact_part_proj_file_order(proj_files_in_part)



def test_packed_io_rejects_output_inside_input(started_cluster):
    # A recursive pack/extract whose output directory is the same as, or nested under, the input
    # directory on the same disk must be rejected: otherwise the directory walk would descend into the
    # files it just generated.
    node.query(
        "CREATE OR REPLACE TABLE t_packed_selfrec (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 's3', min_bytes_for_full_part_storage = '10M'"
    )
    node.query("INSERT INTO t_packed_selfrec SELECT number, toString(number) FROM numbers(10)")

    part_path = get_relative_part_path("t_packed_selfrec", "all_1_1_0")
    nested_output = os.path.join(part_path, "self_output")

    # `clickhouse disks` catches query errors and still exits 0, so assert on the printed error message
    # (captured via 2>&1) rather than on a non-zero exit code / a raised exception.
    output = node.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse disks --config /etc/clickhouse-server/config.xml --disk s3 --query "packed-io --recursive create --disk-from s3 {part_path} {nested_output}" 2>&1',
        ],
        privileged=True,
        user="root",
        nothrow=True,
    )
    assert "nested under the input" in output, f"expected rejection, got: {output}"


def test_packed_io_rejects_output_inside_input_cross_disk(started_cluster):
    # The same filesystem tree is addressable under two disk names: `local` is rooted at `/` and
    # `default` is rooted at the server data path. Feeding the input via `local` (absolute path) and the
    # nested output via `default` (relative path) makes the two disk names differ even though the output
    # is physically nested under the input. The guard must reject this too, so it cannot rely on disk
    # names being equal.
    node.query(
        "CREATE OR REPLACE TABLE t_packed_selfrec_xdisk (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_full_part_storage = '10M'"
    )
    node.query(
        "INSERT INTO t_packed_selfrec_xdisk SELECT number, toString(number) FROM numbers(10)"
    )

    # `local` addresses the part by its absolute filesystem path; `default` by the same tree relative to
    # the server data path.
    abs_part_path = get_part_path("t_packed_selfrec_xdisk", "all_1_1_0")
    default_rel_part_path = get_relative_part_path("t_packed_selfrec_xdisk", "all_1_1_0")
    nested_output = os.path.join(default_rel_part_path, "self_output")

    output = node.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse disks --config /etc/clickhouse-server/config.xml --disk local --query "packed-io --recursive create --disk-from local {abs_part_path} --disk-to default {nested_output}" 2>&1',
        ],
        privileged=True,
        user="root",
        nothrow=True,
    )
    assert "nested under the input" in output, f"expected rejection, got: {output}"


def test_packed_io_create_rejects_output_inside_input(started_cluster):
    # Non-recursive `create` walks the input directory one level deep. If the output archive lands inside
    # that directory, a re-run reads the previously written archive as an input member and nests a stale
    # data.packed inside the new one. The guard must reject an output file whose directory is the input.
    node.query(
        "CREATE OR REPLACE TABLE t_packed_create_selfrec (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = 's3', min_bytes_for_full_part_storage = '10M'"
    )
    node.query(
        "INSERT INTO t_packed_create_selfrec SELECT number, toString(number) FROM numbers(10)"
    )

    part_path = get_relative_part_path("t_packed_create_selfrec", "all_1_1_0")
    output_file = os.path.join(part_path, "data.packed")

    output = node.exec_in_container(
        [
            "bash",
            "-c",
            f'clickhouse disks --config /etc/clickhouse-server/config.xml --disk s3 --query "packed-io create --disk-from s3 {part_path} {output_file}" 2>&1',
        ],
        privileged=True,
        user="root",
        nothrow=True,
    )
    assert "nested under the input" in output, f"expected rejection, got: {output}"
