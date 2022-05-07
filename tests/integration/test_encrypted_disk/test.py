import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry


FIRST_PART_NAME = "all_1_1_0"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    tmpfs=["/disk:size=100M"],
    with_minio=True,
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
        node.query("DROP TABLE IF EXISTS encrypted_test NO DELAY")


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
        SETTINGS storage_policy='{}'
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


# Test adding encryption key on the fly.
def test_add_key():
    def make_storage_policy_with_keys(policy_name, keys):
        node.exec_in_container(
            [
                "bash",
                "-c",
                """cat > /etc/clickhouse-server/config.d/storage_policy_{policy_name}.xml << EOF
<?xml version="1.0"?>
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

    # Add some data to an encrypted disk.
    node.query("SELECT policy_name FROM system.storage_policies")
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys", "<key>firstfirstfirstf</key>"
    )
    assert_eq_with_retry(
        node,
        "SELECT policy_name FROM system.storage_policies WHERE policy_name='encrypted_policy_multikeys'",
        "encrypted_policy_multikeys",
    )

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
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys",
        """
        <key id="0">firstfirstfirstf</key>
        <key id="1">secondsecondseco</key>
        <current_key_id>1</current_key_id>
    """,
    )
    node.query("INSERT INTO encrypted_test VALUES (2,'data'),(3,'data')")

    # Now "(0,'data'),(1,'data')" is encrypted with the first key and "(2,'data'),(3,'data')" is encrypted with the second key.
    # All data are accessible.
    assert node.query(select_query) == "(0,'data'),(1,'data'),(2,'data'),(3,'data')"

    # Try to replace the first key with something wrong, and check that "(0,'data'),(1,'data')" cannot be read.
    make_storage_policy_with_keys(
        "encrypted_policy_multikeys",
        """
        <key id="0">wrongwrongwrongw</key>
        <key id="1">secondsecondseco</key>
        <current_key_id>1</current_key_id>
    """,
    )

    expected_error = "Wrong key"
    assert expected_error in node.query_and_get_error(select_query)

    # Detach the part encrypted with the wrong key and check that another part containing "(2,'data'),(3,'data')" still can be read.
    node.query("ALTER TABLE encrypted_test DETACH PART '{}'".format(FIRST_PART_NAME))
    assert node.query(select_query) == "(2,'data'),(3,'data')"
