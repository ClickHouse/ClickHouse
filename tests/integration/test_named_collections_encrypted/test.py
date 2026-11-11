import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NAMED_COLLECTIONS_CONFIG = os.path.join(
    SCRIPT_DIR, "./configs/config.d/named_collections.xml"
)

ZK_PATH = "/named_collections_path"

# The key of configs/config.d/named_collections_encrypted.xml, and a second key for the rotation test.
KEY_A = "bebec0cabebec0cabebec0cabebec0ca"
KEY_B = "00112233445566778899aabbccddeeff"
LOCAL_ENCRYPTED_CONFIG = (
    "/etc/clickhouse-server/config.d/named_collections_encrypted.xml"
)


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node_encrypted",
            main_configs=[
                "configs/config.d/named_collections_encrypted.xml",
                "configs/config.d/backups.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            external_dirs=["/backups/"],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_with_keeper_encrypted",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_keeper_2_encrypted",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
                "configs/config.d/backups.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
            ],
            external_dirs=["/backups/"],
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def check_encrypted_content(node, zk=None):
    assert (
        "collection1\ncollection2"
        == node.query("select name from system.named_collections").strip()
    )

    assert (
        "['key1','key2']"
        == node.query(
            "select mapKeys(collection) from system.named_collections where name = 'collection2'"
        ).strip()
    )

    assert (
        "1234\tvalue2"
        == node.query(
            "select collection['key1'], collection['key2'] from system.named_collections where name = 'collection2'"
        ).strip()
    )

    # Check that the underlying storage is encrypted
    content = (
        zk.get(ZK_PATH + "/collection2.sql")[0]
        if zk is not None
        else open(
            f"{node.path}/database/named_collections/collection2.sql", "rb"
        ).read()
    )

    assert (
        content[0:3] == b"ENC"
    )  # file signature (aka magic number) of the encrypted file
    assert b"key1" not in content
    assert b"1234" not in content
    assert b"key2" not in content
    assert b"value2" not in content


def test_local_storage_encrypted(cluster):
    node = cluster.instances["node_encrypted"]
    node.query("CREATE NAMED COLLECTION collection2 AS key1=1234, key2='value2'")

    check_encrypted_content(node)
    node.restart_clickhouse()
    check_encrypted_content(node)

    node.query("DROP NAMED COLLECTION collection2")


def test_zookeper_storage_encrypted(cluster):
    node1 = cluster.instances["node_with_keeper_encrypted"]
    node2 = cluster.instances["node_with_keeper_2_encrypted"]
    zk = cluster.get_kazoo_client("zoo1")

    node1.query("CREATE NAMED COLLECTION collection2 AS key1=1234, key2='value2'")

    check_encrypted_content(node1, zk)
    check_encrypted_content(node2, zk)
    node1.restart_clickhouse()
    node2.restart_clickhouse()
    check_encrypted_content(node1, zk)
    check_encrypted_content(node2, zk)

    node1.query("DROP NAMED COLLECTION collection2")


def set_local_storage_keys(node, keys_xml):
    # Rewrites the config of the local encrypted node with the given keys; the rest of the file
    # stays as in configs/config.d/named_collections_encrypted.xml.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""cat > {LOCAL_ENCRYPTED_CONFIG} << 'EOF'
<clickhouse>
  <named_collections_storage>
    <type>local_encrypted</type>
    {keys_xml}
  </named_collections_storage>

  <named_collections>
    <collection1>
      <key1>value1</key1>
    </collection1>
  </named_collections>

  <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
EOF""",
        ]
    )


def key_fingerprint(node, key_hex):
    # The fingerprint of a key as the header of an encrypted entry stores it (little endian).
    return bytes.fromhex(
        node.query(
            "SELECT hex(sipHash128Keyed((0x4368456E63727970::UInt64, 0x7465644469736B46::UInt64), "
            f"unhex('{key_hex}')))"
        ).strip()
    )


def stored_entry_key_fingerprint(node, collection_name):
    with open(
        f"{node.path}/database/named_collections/{collection_name}.sql", "rb"
    ) as f:
        content = f.read()
    assert content[0:3] == b"ENC"
    # The signature (3 bytes), the version (2), the algorithm (2), then the key fingerprint (16).
    return content[7:23]


def test_local_storage_key_rotation(cluster):
    node = cluster.instances["node_encrypted"]
    fingerprint_a = key_fingerprint(node, KEY_A)
    fingerprint_b = key_fingerprint(node, KEY_B)

    # The module config holds key A alone, so the collection is encrypted with it.
    node.query("CREATE NAMED COLLECTION rotated AS key1=1234, key2='value2'")
    assert stored_entry_key_fingerprint(node, "rotated") == fingerprint_a

    # The rotation: key B becomes the current key, key A stays configured for the existing entries.
    set_local_storage_keys(
        node,
        f'<key_hex id="0">{KEY_A}</key_hex><key_hex id="1">{KEY_B}</key_hex>'
        "<current_key_id>1</current_key_id>",
    )
    node.restart_clickhouse()

    # The existing entry is still readable, and is not rewritten by being read.
    assert (
        node.query(
            "SELECT collection['key1'] FROM system.named_collections WHERE name = 'rotated'"
        )
        == "1234\n"
    )
    assert stored_entry_key_fingerprint(node, "rotated") == fingerprint_a

    # New entries are encrypted with the current key.
    node.query("CREATE NAMED COLLECTION fresh AS key1=1")
    assert stored_entry_key_fingerprint(node, "fresh") == fingerprint_b

    # A backup holding entries encrypted with both keys: each entry is authenticated with its own key.
    node.query("BACKUP TABLE system.named_collections TO File('/backups/rotation')")

    # Rewriting an entry re-encrypts it with the current key.
    node.query("ALTER NAMED COLLECTION rotated SET key1=4321")
    assert stored_entry_key_fingerprint(node, "rotated") == fingerprint_b

    # The restore decrypts each entry with the key its header names and re-creates it with the current key.
    node.query("DROP NAMED COLLECTION rotated")
    node.query("DROP NAMED COLLECTION fresh")
    node.query("RESTORE TABLE system.named_collections FROM File('/backups/rotation')")
    assert (
        node.query(
            "SELECT name, collection['key1'] FROM system.named_collections "
            "WHERE name IN ('rotated', 'fresh') ORDER BY name"
        )
        == "fresh\t1\nrotated\t1234\n"
    )
    assert stored_entry_key_fingerprint(node, "rotated") == fingerprint_b
    assert stored_entry_key_fingerprint(node, "fresh") == fingerprint_b

    # Once nothing is encrypted with key A any more, it can be removed from the configuration.
    set_local_storage_keys(node, f"<key_hex>{KEY_B}</key_hex>")
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT name FROM system.named_collections WHERE name IN ('rotated', 'fresh') ORDER BY name"
        )
        == "fresh\nrotated\n"
    )

    # The backup still holds an entry encrypted with key A: without that key the restore fails before
    # creating anything, naming the fingerprint of the missing key.
    node.query("DROP NAMED COLLECTION rotated")
    node.query("DROP NAMED COLLECTION fresh")
    error = node.query_and_get_error(
        "RESTORE TABLE system.named_collections FROM File('/backups/rotation')"
    )
    assert "encrypted with a key with fingerprint" in error
    assert fingerprint_a[::-1].hex() in error
    assert (
        node.query(
            "SELECT count() FROM system.named_collections WHERE name IN ('rotated', 'fresh')"
        )
        == "0\n"
    )

    # Back to the module's configuration for the other tests; nothing stored is encrypted with key B.
    set_local_storage_keys(node, f"<key_hex>{KEY_A}</key_hex>")
    node.restart_clickhouse()


def test_local_storage_missing_key(cluster):
    node = cluster.instances["node_encrypted"]

    # A backup holding `locked` encrypted with key B, with a value other than the one stored below.
    set_local_storage_keys(
        node,
        f'<key_hex id="0">{KEY_A}</key_hex><key_hex id="1">{KEY_B}</key_hex>'
        "<current_key_id>1</current_key_id>",
    )
    node.restart_clickhouse()
    node.query("CREATE NAMED COLLECTION locked AS key1=2")
    node.query("BACKUP TABLE system.named_collections TO File('/backups/missing_key')")
    node.query("DROP NAMED COLLECTION locked")

    # The stored `locked` is encrypted with key A.
    set_local_storage_keys(node, f"<key_hex>{KEY_A}</key_hex>")
    node.restart_clickhouse()
    node.query("CREATE NAMED COLLECTION locked AS key1=1")
    node.query("CREATE NAMED COLLECTION locked2 AS key1=1")

    # Key A is removed from the configuration before `locked` is rewritten with another key. The
    # server still starts, and the collections it can decrypt are available; `locked` is skipped
    # with an error in the log and stays in the storage.
    set_local_storage_keys(node, f"<key_hex>{KEY_B}</key_hex>")
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT name FROM system.named_collections WHERE name IN ('collection1', 'locked', 'locked2') ORDER BY name"
        )
        == "collection1\n"
    )
    assert node.contains_in_log("Cannot read named collection 'locked', skipping it")
    assert node.contains_in_log("Cannot read named collection 'locked2', skipping it")
    node.query("CREATE NAMED COLLECTION fresh AS key1=3")

    # The retained entry is still a named collection: it is not created again, not replaced, and
    # `IF NOT EXISTS` keeps it. A restore keeps it in the default mode and refuses to replace it.
    assert "already exists" in node.query_and_get_error("CREATE NAMED COLLECTION locked AS key1=2")
    assert "already exists" in node.query_and_get_error(
        "CREATE OR REPLACE NAMED COLLECTION locked AS key1=2"
    )
    node.query("CREATE NAMED COLLECTION IF NOT EXISTS locked AS key1=2")
    node.query("RESTORE TABLE system.named_collections FROM File('/backups/missing_key')")
    assert "NAMED_COLLECTION_ALREADY_EXISTS" in node.query_and_get_error(
        "RESTORE TABLE system.named_collections FROM File('/backups/missing_key') "
        "SETTINGS create_named_collection = 'replace'"
    )
    assert (
        node.query("SELECT count() FROM system.named_collections WHERE name = 'locked'")
        == "0\n"
    )

    # Dropping a retained entry is the way to get rid of it: the name is free again.
    node.query("DROP NAMED COLLECTION locked2")
    node.query("CREATE NAMED COLLECTION locked2 AS key1=5")

    # Once key A is configured again, `locked` is back with its original value.
    set_local_storage_keys(
        node,
        f'<key_hex id="0">{KEY_A}</key_hex><key_hex id="1">{KEY_B}</key_hex>'
        "<current_key_id>1</current_key_id>",
    )
    node.restart_clickhouse()
    assert (
        node.query(
            "SELECT name, collection['key1'] FROM system.named_collections "
            "WHERE name IN ('locked', 'locked2', 'fresh') ORDER BY name"
        )
        == "fresh\t3\nlocked\t1\nlocked2\t5\n"
    )

    # Back to the module's configuration for the other tests.
    node.query("DROP NAMED COLLECTION locked")
    node.query("DROP NAMED COLLECTION locked2")
    node.query("DROP NAMED COLLECTION fresh")
    set_local_storage_keys(node, f"<key_hex>{KEY_A}</key_hex>")
    node.restart_clickhouse()


ZOOKEEPER_ENCRYPTED_CONFIG = (
    "/etc/clickhouse-server/config.d/named_collections_with_zookeeper_encrypted.xml"
)


def set_zookeeper_storage_keys(node, keys_xml):
    # Rewrites the config of a Keeper node with the given keys; the rest of the file stays as in
    # configs/config.d/named_collections_with_zookeeper_encrypted.xml.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""cat > {ZOOKEEPER_ENCRYPTED_CONFIG} << 'EOF'
<clickhouse>
  <named_collections_storage>
    <type>zookeeper_encrypted</type>
    {keys_xml}
    <path>/named_collections_path/</path>
    <update_timeout_ms>5000</update_timeout_ms>
  </named_collections_storage>

  <named_collections>
    <collection1>
      <key1>value1</key1>
    </collection1>
  </named_collections>

  <remote_servers>
      <replicated_nc_nodes_cluster>
          <shard>
              <internal_replication>true</internal_replication>
              <replica>
                  <host>node_with_keeper</host>
                  <port>9000</port>
              </replica>
              <replica>
                  <host>node_with_keeper_2</host>
                  <port>9000</port>
              </replica>
          </shard>
          <allow_distributed_ddl_queries>true</allow_distributed_ddl_queries>
      </replicated_nc_nodes_cluster>
  </remote_servers>

  <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
EOF""",
        ]
    )


def test_zookeeper_storage_missing_key(cluster):
    node1 = cluster.instances["node_with_keeper_encrypted"]
    node2 = cluster.instances["node_with_keeper_2_encrypted"]

    # node1 writes with key A. A backup holding `visible` encrypted with key B, taken while node2
    # holds both keys.
    set_zookeeper_storage_keys(node1, f"<key_hex>{KEY_A}</key_hex>")
    node1.restart_clickhouse()
    set_zookeeper_storage_keys(
        node2,
        f'<key_hex id="0">{KEY_A}</key_hex><key_hex id="1">{KEY_B}</key_hex>'
        "<current_key_id>1</current_key_id>",
    )
    node2.restart_clickhouse()
    node2.query("CREATE NAMED COLLECTION visible AS key1=1")
    node2.query("BACKUP TABLE system.named_collections TO File('/backups/disclosure')")
    node2.query("DROP NAMED COLLECTION visible")

    # An entry encrypted with key A written into the shared storage by node1 while node2 holds
    # key B only: node2 skips it.
    set_zookeeper_storage_keys(node2, f"<key_hex>{KEY_B}</key_hex>")
    node2.restart_clickhouse()
    node1.query("CREATE NAMED COLLECTION locked AS key1=1")

    # A user who may restore named collections, but not see their names, restores the backup on
    # node2 and receives the log records of the query. The restore reloads the shared storage,
    # which skips `locked` again; its name reaches the server log only.
    node2.query("CREATE USER restorer NOT IDENTIFIED")
    node2.query("GRANT CREATE NAMED COLLECTION ON * TO restorer")
    node2.query("GRANT READ ON FILE TO restorer")  # the right reading a File() backup needs
    out, err = node2.query_and_get_answer_with_error(
        "RESTORE TABLE system.named_collections FROM File('/backups/disclosure')",
        user="restorer",
        settings={"send_logs_level": "error"},
    )
    assert "RESTORED" in out
    assert "locked" not in err
    assert node2.contains_in_log("Cannot read named collection 'locked', skipping it")
    assert (
        node2.query(
            "SELECT collection['key1'] FROM system.named_collections WHERE name = 'visible'"
        )
        == "1\n"
    )

    # Back to the module's configuration for the other tests.
    node2.query("DROP USER restorer")
    node2.query("DROP NAMED COLLECTION visible")
    node1.query("DROP NAMED COLLECTION locked")
    set_zookeeper_storage_keys(node2, f"<key_hex>{KEY_A}</key_hex>")
    node2.restart_clickhouse()
