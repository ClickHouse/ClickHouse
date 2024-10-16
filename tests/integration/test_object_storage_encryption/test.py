import os

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=["configs/storage.xml"],
            external_dirs=["/vardisks/"],
            with_minio=True,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


disks = {
    "encrypted": [("encrypted", False, "enc")],
    "cached": [
        ("encrypted_cached", False, "enc"),
        ("cached", True, "cache"),
    ],
    "cached_write": [
        ("encrypted_cached_write", False, "enc"),
        ("cached_write", False, "cache"),
    ],
    "header_cache": [
        ("encrypted_header_cache", False, "enc"),
        ("header_cache", True, "header"),
    ],
    "header_cache_write": [
        ("encrypted_header_cache_write", False, "enc"),
        ("header_cache_write", False, "header"),
    ],
    # "cache_with_header_cache": [
    #    ("encrypted_cache_with_header_cache", False, "enc"),
    #    ("cache_with_header_cache", True, "header"),
    #    ("cached_with_header_cache", True, "cache"),
    # ],
    # "cache_with_header_cache_write": [
    #    ("encrypted_cache_with_header_cache_write", False, "enc"),
    #    ("cache_with_header_cache_write", False, "header"),
    #    ("cached_with_header_cache_write", True, "cache"),
    # ],
    # "cache_write_with_header_cache": [
    #    ("encrypted_cache_write_with_header_cache", False, "enc"),
    #    ("cache_write_with_header_cache", True, "header"),
    #    ("cached_write_with_header_cache", False, "cache"),
    # ],
    # "cache_write_with_header_cache_write": [
    #    ("encrypted_cache_write_with_header_cache_write", False, "enc"),
    #    ("cache_write_with_header_cache_write", False, "header"),
    #    ("cached_write_with_header_cache_write", False, "cache"),
    # ],
    "s3encrypted": [],
    "s3cached": [
        ("s3_cached", True, "cache"),
    ],
    "s3cached_write": [
        ("s3_cached_write", False, "cache"),
    ],
    "s3header_cache": [
        ("s3_header_cache", True, "header"),
    ],
    "s3header_cache_write": [
        ("s3_header_cache_write", False, "header"),
    ],
    # "s3cache_with_header_cache": [
    #    ("s3_cache_with_header_cache", True, "header"),
    #    ("s3_cached_with_header_cache", True, "cache"),
    # ],
    # "s3cache_with_header_cache_write": [
    #    ("s3_cache_with_header_cache_write", False, "header"),
    #    ("s3_cached_with_header_cache_write", True, "cache"),
    # ],
    # "s3cache_write_with_header_cache": [
    #    ("s3_cache_write_with_header_cache", True, "header"),
    #    ("s3_cached_write_with_header_cache", False, "cache"),
    # ],
    # "s3cache_write_with_header_cache_write": [
    #    ("s3_cache_write_with_header_cache_write", False, "header"),
    #    ("s3_cached_write_with_header_cache_write", False, "cache"),
    # ],
}

# headers won't be cached on read if the full file is already cached
ingore_empty = ["cache_write_with_header_cache", "s3_cache_write_with_header_cache"]
HEADER_SIZE = 64


@pytest.fixture(autouse=True)
def cleanup_after_test(cluster):
    try:
        yield
    finally:
        node = cluster.instances["node"]
        node.query("DROP TABLE IF EXISTS encrypted_test SYNC")


def helper_check_all_files_encrypted(path, header_only=False):
    for root, _, files in os.walk(path):
        for fname in files:
            f = os.path.join(root, fname)
            print(f"Checking {f}")
            if f.endswith("status"):
                continue
            with open(f, "rb") as fd:
                assert fd.read(3) == b"ENC"
            if header_only:
                assert os.path.getsize(f) == HEADER_SIZE
            else:
                assert os.path.getsize(f) >= HEADER_SIZE


def helper_check_no_files_encrypted(path):
    for root, _, files in os.walk(path):
        for fname in files:
            f = os.path.join(root, fname)
            with open(f, "rb") as fd:
                assert fd.read(3) != b"ENC"


def is_empty(path):
    if not os.path.exists(path):
        return True
    # account 1 for status file
    if len(os.listdir(path)) <= 1:
        return True
    for file in os.listdir(path):
        print(f"Found file: {path}/{file}")
    return False


def assert_empty(path):
    assert is_empty(path)


def assert_not_empty(path):
    assert not is_empty(path)


def helper_checks(node, policy, pre_select):
    root_path = os.path.join(node.cluster.instances_dir, "vardisks")
    for name, after_select, disk_type in disks[policy]:
        disk = os.path.join(root_path, name)
        if pre_select and after_select:
            assert_empty(disk)
        else:
            if name not in ingore_empty:
                assert_not_empty(disk)
            if disk_type == "enc":
                helper_check_all_files_encrypted(disk)
            elif disk_type == "cache":
                helper_check_no_files_encrypted(disk)
            elif disk_type == "header":
                helper_check_all_files_encrypted(disk, header_only=True)
            else:
                assert False
    for current_policy, disks_list in disks.items():
        if current_policy != policy:
            for name, _, disk_type in disks_list:
                if disk_type != "cache" and disk_type != "header":
                    disk = os.path.join(root_path, name)
                    assert_empty(disk)


def helper_first_insert_check(node, policy):
    helper_checks(node, policy, True)


def helper_after_select_check(node, policy):
    helper_checks(node, policy, False)


@pytest.mark.parametrize("policy", disks.keys())
def test_encrypted_os_disk(cluster, policy):
    node = cluster.instances["node"]
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            policy
        )
    )

    node.query(
        "INSERT INTO encrypted_test SETTINGS enable_filesystem_cache_on_write_operations=1 VALUES (0,'data'),(1,'data')"
    )

    helper_first_insert_check(node, policy)

    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    helper_after_select_check(node, policy)

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    node.query("OPTIMIZE TABLE encrypted_test FINAL")
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    node.restart_clickhouse()

    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"
    node.query("INSERT INTO encrypted_test VALUES (4,'data'),(5,'data')")
    assert (
        node.query(select_query)
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data'),(4,'data'),(5,'data')"
    )
    node.query("OPTIMIZE TABLE encrypted_test FINAL")
    assert (
        node.query(select_query)
        == "(0,'data'),(1,'data'),(2,'data'),(3,'data'),(4,'data'),(5,'data')"
    )

    helper_after_select_check(node, policy)


@pytest.mark.parametrize(
    "policy, destination_disks",
    [
        (
            "mutliple_disks_local",
            [
                "locobj_encrypted",
                "disk_local",
            ],
        ),
        ("mutliple_disks_local_os", ["locobj_encrypted", "locobj2"]),
        ("s3_mutliple_disks_local", ["s3obj_encrypted", "disk_local"]),
        ("s3_mutliple_disks_s3", ["s3obj_encrypted", "s3plain"]),
    ],
)
def test_part_move(cluster, policy, destination_disks):
    node = cluster.instances["node"]
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}'
        """.format(
            policy
        )
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    part_name_tmpl = "all_1_1_{}"
    part_count = 0
    for destination_disk in destination_disks:
        part_name = part_name_tmpl.format(part_count)
        part_count += 1
        node.query(
            "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
                part_name, destination_disk
            )
        )
        assert node.query(select_query) == "(0,'data'),(1,'data')"
        with pytest.raises(QueryRuntimeException) as exc:
            node.query(
                "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
                    part_name, destination_disk
                )
            )
        assert "Part '{}' is already on disk '{}'".format(
            part_name, destination_disk
        ) in str(exc.value)
        node.query("OPTIMIZE TABLE encrypted_test FINAL")
        assert node.query(select_query) == "(0,'data'),(1,'data')"


@pytest.mark.parametrize("policy", ["encrypted", "s3encrypted"])
def test_log_family(cluster, policy):
    node = cluster.instances["node"]
    node.query(
        f"""
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=Log
        SETTINGS storage_policy='{policy}'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"backup{backup_id_counter}"


@pytest.mark.parametrize(
    "policies",
    [
        ("cached", "encrypted", "File"),
        ("s3cached", "s3encrypted", "File"),
        ("cached", "encrypted", "S3"),
        ("s3cached", "s3encrypted", "S3"),
    ],
)
def test_backup_restore(cluster, policies):
    backup_dest_tmpl = "File('/{backup_destination}')"
    if policies[2] == "S3":
        backup_dest_tmpl = "S3('http://minio1:9001/root/backups/{backup_destination}', 'minio', 'minio123')"
    node = cluster.instances["node"]
    node.query(
        f"""
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{policies[0]}'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"
    backup_destination = "vardisks/backups/" + new_backup_name()
    backup_dest_sql = backup_dest_tmpl.format(backup_destination=backup_destination)
    node.query(
        f"BACKUP TABLE encrypted_test TO {backup_dest_sql} SETTINGS decrypt_files_from_encrypted_disks=1"
    )

    if policies[2] != "S3":
        root_path = os.path.join(node.cluster.instances_dir, backup_destination)
        with open(f"{root_path}/metadata/default/encrypted_test.sql") as file:
            assert file.read().startswith("CREATE TABLE default.encrypted_test")
        with open(f"{root_path}/.backup") as file:
            assert file.read().find("<encrypted_by_disk>true</encrypted_by_disk>") == -1
        helper_check_no_files_encrypted(f"{root_path}/.backup")

    node.query(f"DROP TABLE encrypted_test SYNC")
    node.query(
        f"""
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{policies[0]}'
        """
    )
    node.query(
        f"RESTORE TABLE encrypted_test FROM {backup_dest_sql} SETTINGS allow_different_table_def=0"
    )
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query(f"DROP TABLE encrypted_test SYNC")
    node.query(
        f"""
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{policies[1]}'
        """
    )
    node.query(
        f"RESTORE TABLE encrypted_test FROM {backup_dest_sql} SETTINGS allow_different_table_def=1"
    )
    assert node.query(select_query) == "(0,'data'),(1,'data')"
