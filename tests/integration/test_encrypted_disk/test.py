import os.path

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

FIRST_PART_NAME = "all_1_1_0"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml", "configs/allow_backup_path.xml"],
    tmpfs=["/disk:size=100M"],
    external_dirs=["/backups/"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS encrypted_test SYNC")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"backup{backup_id_counter}"


@pytest.mark.parametrize(
    "policy",
    ["encrypted_policy", "encrypted_policy_key192b", "local_policy", "s3_policy"],
)
def test_encrypted_disk(policy):
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

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    node.query("OPTIMIZE TABLE encrypted_test FINAL")
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"


@pytest.mark.parametrize(
    "policy, destination_disks",
    [
        (
            "local_policy",
            [
                "disk_local_encrypted",
                "disk_local_encrypted2",
                "disk_local_encrypted_key192b",
                "disk_local",
            ],
        ),
        ("s3_policy", ["disk_s3_encrypted", "disk_s3"]),
    ],
)
def test_part_move(policy, destination_disks):
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{}', temporary_directories_lifetime=1
        """.format(
            policy
        )
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    for destination_disk in destination_disks:
        node.query(
            "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
                FIRST_PART_NAME, destination_disk
            )
        )
        assert node.query(select_query) == "(0,'data'),(1,'data')"
        with pytest.raises(QueryRuntimeException) as exc:
            node.query(
                "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
                    FIRST_PART_NAME, destination_disk
                )
            )
        assert "Part '{}' is already on disk '{}'".format(
            FIRST_PART_NAME, destination_disk
        ) in str(exc.value)

    assert node.query(select_query) == "(0,'data'),(1,'data')"


@pytest.mark.parametrize(
    "policy,encrypted_disk",
    [("local_policy", "disk_local_encrypted"), ("s3_policy", "disk_s3_encrypted")],
)
def test_optimize_table(policy, encrypted_disk):
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

    node.query(
        "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
            FIRST_PART_NAME, encrypted_disk
        )
    )
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    node.query("OPTIMIZE TABLE encrypted_test FINAL")

    with pytest.raises(QueryRuntimeException) as exc:
        node.query(
            "ALTER TABLE encrypted_test MOVE PART '{}' TO DISK '{}'".format(
                FIRST_PART_NAME, encrypted_disk
            )
        )

    assert "Part {} is not exists or not active".format(FIRST_PART_NAME) in str(
        exc.value
    )

    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"


def make_storage_policy_with_keys(
    policy_name, keys, check_system_storage_policies=False
):
    if check_system_storage_policies:
        node.query("SELECT policy_name FROM system.storage_policies")

    node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/storage_policy_{policy_name}.xml << EOF
<clickhouse>
<storage_configuration>
    <disks>
        <{policy_name}_disk>
            <type>encrypted</type>
            <disk>disk_local</disk>
            <path>{policy_name}_dir/</path>
            {keys}
        </{policy_name}_disk>
    </disks>
    <policies>
        <{policy_name}>
            <volumes>
                <main>
                    <disk>{policy_name}_disk</disk>
                </main>
            </volumes>
        </{policy_name}>
        </policies>
</storage_configuration>
</clickhouse>
EOF""".format(
                policy_name=policy_name, keys=keys
            ),
        ]
    )

    node.query("SYSTEM RELOAD CONFIG")

    if check_system_storage_policies:
        assert_eq_with_retry(
            node,
            f"SELECT policy_name FROM system.storage_policies WHERE policy_name='{policy_name}'",
            policy_name,
        )


# Test adding encryption key on the fly.
def test_add_keys():
    keys = "<key>firstfirstfirstf</key>"
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys", keys, check_system_storage_policies=True
    )

    # Add some data to an encrypted disk.
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='encrypted_policy_multikeys'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    # Add a second key and start using it.
    keys = """
        <key>firstfirstfirstf</key>
        <key>secondsecondseco</key>
        <current_key>secondsecondseco</current_key>
        """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")

    # Now "(0,'data'),(1,'data')" is encrypted with the first key and "(2,'data'),(3,'data')" is encrypted with the second key.
    # All data are accessible.
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Keys can be reordered.
    keys = """
        <key id="1">secondsecondseco</key>
        <key id="0">firstfirstfirstf</key>
        <current_key_id>1</current_key_id>
        """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    # All data are still accessible.
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Try to replace the first key with something wrong, and check that "(0,'data'),(1,'data')" cannot be read.
    keys = """
        <key>secondsecondseco</key>
        <key>wrongwrongwrongw</key>
        <current_key>secondsecondseco</current_key>
    """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    expected_error = "Not found an encryption key required to decipher"
    assert expected_error in node.query_and_get_error(select_query)

    # Detach the part encrypted with the wrong key and check that another part containing "(2,'data'),(3,'data')" still can be read.
    node.query("ALTER TABLE encrypted_test DETACH PART '{}'".format(FIRST_PART_NAME))
    assert node.query(select_query) == "(2,'data'),(3,'data')"


# Test adding encryption key on the fly.
def test_add_keys_with_id():
    keys = "<key>firstfirstfirstf</key>"
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys", keys, check_system_storage_policies=True
    )

    # Add some data to an encrypted disk.
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='encrypted_policy_multikeys'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    # Add a second key and start using it.
    keys = """
        <key id="0">firstfirstfirstf</key>
        <key id="1">secondsecondseco</key>
        <current_key_id>1</current_key_id>
        """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")

    # Now "(0,'data'),(1,'data')" is encrypted with the first key and "(2,'data'),(3,'data')" is encrypted with the second key.
    # All data are accessible.
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Keys can be reordered.
    keys = """
        <key id="1">secondsecondseco</key>
        <key id="0">firstfirstfirstf</key>
        <current_key_id>1</current_key_id>
        """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    # All data are still accessible.
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Try to replace the first key with something wrong, and check that "(0,'data'),(1,'data')" cannot be read.
    keys = """
        <key id="1">secondsecondseco</key>
        <key id="0">wrongwrongwrongw</key>
        <current_key_id>1</current_key_id>
    """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    expected_error = "Not found an encryption key required to decipher"
    assert expected_error in node.query_and_get_error(select_query)

    # Detach the part encrypted with the wrong key and check that another part containing "(2,'data'),(3,'data')" still can be read.
    node.query("ALTER TABLE encrypted_test DETACH PART '{}'".format(FIRST_PART_NAME))
    assert node.query(select_query) == "(2,'data'),(3,'data')"


# Test appending of encrypted files.
def test_log_family():
    keys = "<key>firstfirstfirstf</key>"
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys", keys, check_system_storage_policies=True
    )

    # Add some data to an encrypted disk.
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=Log
        SETTINGS storage_policy='encrypted_policy_multikeys'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    # Add a second key and start using it.
    keys = """
        <key>firstfirstfirstf</key>
        <key>secondsecondseco</key>
        <current_key>secondsecondseco</current_key>
        """
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Everything is still encrypted with the first key (because the Log engine appends files), so the second key can be removed.
    keys = "<key>firstfirstfirstf</key>"
    make_storage_policy_with_keys("encrypted_policy_multikeys", keys)

    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"


@pytest.mark.parametrize(
    "old_version",
    ["version_1le", "version_1be", "version_2"],
)
def test_migration_from_old_version(old_version):
    keys = """
        <key id="1">first_key_first_</key>
        <key id="2">second_key_secon</key>
        <key id="3">third_key_third_</key>
        <current_key_id>3</current_key_id>
    """
    make_storage_policy_with_keys(
        "migration_from_old_version", keys, check_system_storage_policies=True
    )

    # Create a table without data.
    node.query(
        """
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=Log
        SETTINGS storage_policy='migration_from_old_version'
        """
    )

    # Copy table's data from an old version.
    data_path = node.query(
        "SELECT data_paths[1] FROM system.tables WHERE table = 'encrypted_test'"
    ).splitlines()[0]
    node.query("DETACH TABLE encrypted_test")

    old_version_dir = os.path.join(SCRIPT_DIR, "old_versions", old_version)
    for file_name in os.listdir(old_version_dir):
        src_path = os.path.join(old_version_dir, file_name)
        dest_path = os.path.join(data_path, file_name)
        node.copy_file_to_container(src_path, dest_path)

    node.query("ATTACH TABLE encrypted_test")

    # We can read from encrypted disk after migration.
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'ab'),(1,'cdefg')"

    # We can append files on encrypted disk after migration.
    node.query("INSERT INTO encrypted_test VALUES (2,'xyz')")
    assert node.query(select_query) == "(0,'ab'),(1,'cdefg'),(2,'xyz')"


def test_read_in_order():
    node.query(
        "CREATE TABLE encrypted_test(`a` UInt64,  `b` String(150)) ENGINE = MergeTree() ORDER BY (a, b) SETTINGS storage_policy='encrypted_policy'"
    )

    node.query(
        "INSERT INTO encrypted_test SELECT * FROM generateRandom('a UInt64, b FixedString(150)') LIMIT 100000"
    )

    node.query(
        "SELECT * FROM encrypted_test ORDER BY a, b SETTINGS optimize_read_in_order=1 FORMAT Null"
    )

    node.query(
        "SELECT * FROM encrypted_test ORDER BY a, b SETTINGS optimize_read_in_order=0 FORMAT Null"
    )


def test_restart():
    for policy in ["disk_s3_encrypted_default_path", "encrypted_s3_cache"]:
        node.query(
            f"""
            DROP TABLE IF EXISTS encrypted_test;
            CREATE TABLE encrypted_test (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS disk='{policy}'
            """
        )

        node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
        select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
        assert node.query(select_query) == "(0,'data'),(1,'data')"

        node.restart_clickhouse()

        assert node.query(select_query) == "(0,'data'),(1,'data')"

        node.query("DROP TABLE encrypted_test SYNC;")


@pytest.mark.parametrize(
    "backup_type,old_storage_policy,new_storage_policy,decrypt_files_from_encrypted_disks",
    [
        ("S3", "encrypted_policy", "encrypted_policy", False),
        ("S3", "encrypted_policy", "s3_encrypted_default_path", False),
        ("S3", "s3_encrypted_default_path", "s3_encrypted_default_path", False),
        ("S3", "s3_encrypted_default_path", "encrypted_policy", False),
        ("File", "s3_encrypted_default_path", "encrypted_policy", False),
        ("File", "local_policy", "encrypted_policy", False),
        ("File", "encrypted_policy", "local_policy", False),
        ("File", "encrypted_policy", "local_policy", True),
    ],
)
def test_backup_restore(
    backup_type,
    old_storage_policy,
    new_storage_policy,
    decrypt_files_from_encrypted_disks,
):
    node.query(
        f"""
        CREATE TABLE encrypted_test (
            id Int64,
            data String
        ) ENGINE=MergeTree()
        ORDER BY id
        SETTINGS storage_policy='{old_storage_policy}'
        """
    )

    node.query("INSERT INTO encrypted_test VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    backup_name = new_backup_name()
    if backup_type == "S3":
        backup_destination = (
            f"S3('http://minio1:9001/root/backups/{backup_name}', 'minio', 'minio123')"
        )
    elif backup_type == "File":
        backup_destination = f"File('/backups/{backup_name}/')"

    node.query(
        f"BACKUP TABLE encrypted_test TO {backup_destination} SETTINGS decrypt_files_from_encrypted_disks={int(decrypt_files_from_encrypted_disks)}"
    )

    storage_policy_changed = old_storage_policy != new_storage_policy
    old_disk_encrypted = old_storage_policy.find("encrypted") != -1
    new_disk_encrypted = new_storage_policy.find("encrypted") != -1

    if backup_type == "File":
        root_path = os.path.join(node.cluster.instances_dir, "backups", backup_name)
        expect_encrypted_in_backup = (
            old_disk_encrypted and not decrypt_files_from_encrypted_disks
        )

        with open(f"{root_path}/metadata/default/encrypted_test.sql") as file:
            assert file.read().startswith("CREATE TABLE default.encrypted_test")

        with open(f"{root_path}/.backup") as file:
            found_encrypted_in_backup = (
                file.read().find("<encrypted_by_disk>true</encrypted_by_disk>") != -1
            )
            assert found_encrypted_in_backup == expect_encrypted_in_backup

        with open(
            f"{root_path}/data/default/encrypted_test/all_1_1_0/data.bin", "rb"
        ) as file:
            found_encrypted_in_backup = file.read().startswith(b"ENC")
            assert found_encrypted_in_backup == expect_encrypted_in_backup

    node.query(f"DROP TABLE encrypted_test SYNC")

    if storage_policy_changed:
        node.query(
            f"""
            CREATE TABLE encrypted_test (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='{new_storage_policy}'
            """
        )

    restore_command = f"RESTORE TABLE encrypted_test FROM {backup_destination} SETTINGS allow_different_table_def={int(storage_policy_changed)}"

    expect_error = None
    if (
        old_disk_encrypted
        and not new_disk_encrypted
        and not decrypt_files_from_encrypted_disks
    ):
        expect_error = "can be restored only to an encrypted disk"

    if expect_error:
        assert expect_error in node.query_and_get_error(restore_command)
    else:
        node.query(restore_command)
        assert node.query(select_query) == "(0,'data'),(1,'data')"
