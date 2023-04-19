import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def make_storage_with_key(id):
    node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/storage_keys_config.xml << EOF
<?xml version="1.0"?>
<clickhouse>
    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex id="0">83e84e9a4eb11535c0670dc62d808ee0</key_hex>
            <key id="1">abcdefghijklmnop</key>
            <current_key_id>{cur_id}</current_key_id>
        </aes_128_gcm_siv>
        <aes_256_gcm_siv>
            <key_hex id="0">83e84e9a4eb11535c0670dc62d808ee083e84e9a4eb11535c0670dc62d808ee0</key_hex>
            <key id="1">abcdefghijklmnopabcdefghijklmnop</key>
            <current_key_id>{cur_id}</current_key_id>
        </aes_256_gcm_siv>
    </encryption_codecs>
</clickhouse>
EOF""".format(
                cur_id=id
            ),
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_different_keys(start_cluster):
    make_storage_with_key(0)
    node.query(
        """
        CREATE TABLE encrypted_test_128 (
            id Int64,
            data String Codec(AES_128_GCM_SIV)
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    node.query(
        """
        CREATE TABLE encrypted_test_256 (
            id Int64,
            data String Codec(AES_256_GCM_SIV)
        ) ENGINE=MergeTree()
        ORDER BY id
        """
    )

    node.query("INSERT INTO encrypted_test_128 VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test_128 ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    make_storage_with_key(1)
    node.query("INSERT INTO encrypted_test_128 VALUES (3,'text'),(4,'text')")
    select_query = "SELECT * FROM encrypted_test_128 ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data'),(3,'text'),(4,'text')"

    node.query("INSERT INTO encrypted_test_256 VALUES (0,'data'),(1,'data')")
    select_query = "SELECT * FROM encrypted_test_256 ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data')"

    make_storage_with_key(1)
    node.query("INSERT INTO encrypted_test_256 VALUES (3,'text'),(4,'text')")
    select_query = "SELECT * FROM encrypted_test_256 ORDER BY id FORMAT Values"
    assert node.query(select_query) == "(0,'data'),(1,'data'),(3,'text'),(4,'text')"
