import json
import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_check_part_with_cache(start_cluster):
    if node.is_built_with_sanitizer() or node.is_debug_build():
        pytest.skip(
            "Skip with debug build and sanitizers. \
            This test manually corrupts cache which triggers LOGICAL_ERROR \
            and leads to crash with those builds"
        )

    node.query(
        """
        CREATE TABLE s3_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='s3_cache'
        """
    )

    node.query("SYSTEM STOP MERGES s3_test")

    node.query(
        "INSERT INTO s3_test VALUES (0, 'data')",
        settings={"enable_filesystem_cache_on_write_operations": 1},
    )

    node.query(
        "INSERT INTO s3_test VALUES (1, 'data')",
        settings={"enable_filesystem_cache_on_write_operations": 1},
    )

    def get_cache_path_of_data_file(part_name):
        disk_path = node.query(
            "SELECT path FROM system.disks WHERE name = 's3_cache'"
        ).strip("\n")

        part_path = node.query(
            f"SELECT path FROM system.parts WHERE table = 's3_test' AND name = '{part_name}'"
        ).strip("\n")

        local_data_file_path = os.path.relpath(part_path, disk_path) + "/data.bin"

        return node.query(
            f"SELECT cache_paths[1] FROM system.remote_data_paths WHERE disk_name = 's3_cache' AND local_path = '{local_data_file_path}'"
        ).strip("\n")

    cache_path = get_cache_path_of_data_file("all_1_1_0")
    assert len(cache_path) > 0

    node.exec_in_container(
        ["bash", "-c", f"truncate -s -1 {cache_path}"], privileged=True
    )

    assert (
        node.query(
            "SELECT count() FROM s3_test WHERE NOT ignore(*)",
            settings={"enable_filesystem_cache": 0},
        )
        == "2\n"
    )

    with pytest.raises(Exception):
        node.query(
            "SELECT count() FROM s3_test WHERE NOT ignore(*)",
            settings={"enable_filesystem_cache": 1},
        )

    assert node.query("CHECK TABLE s3_test") == "1\n"

    # Check that cache is removed only for one part after CHECK TABLE
    cache_path = get_cache_path_of_data_file("all_1_1_0")
    assert len(cache_path) == 0

    cache_path = get_cache_path_of_data_file("all_2_2_0")
    assert len(cache_path) > 0

    assert (
        node.query(
            "SELECT count() FROM s3_test WHERE NOT ignore(*)",
            settings={"enable_filesystem_cache": 1},
        )
        == "2\n"
    )
