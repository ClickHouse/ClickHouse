import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": "s1", "replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": "s1", "replica": "node2"},
)

ZK_PATH = "/test/rename_dictionary_lag"
FAILPOINT = "database_replicated_stop_entry_execution"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_lagging_replica_replays_rename_as_user(started_cluster):
    # node1 has not applied node2's CREATE DICTIONARY when it accepts the RENAME, so the check before the enqueue
    # sees no dictionary; only the replay, run as the user after node1 catches up, can refuse it.
    try:
        for node in (node1, node2):
            node.query("CREATE USER u IDENTIFIED WITH plaintext_password BY 'p'")
            node.query(
                f"CREATE DATABASE rd ENGINE = Replicated('{ZK_PATH}', '{{shard}}', '{{replica}}')"
            )
        node2.query_with_retry(
            "SELECT count() FROM system.databases WHERE name = 'rd'",
            check_callback=lambda x: x.strip() == "1",
        )
        for node in (node1, node2):
            node.query("GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON rd.* TO u")

        node1.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
        node2.query(
            "CREATE DICTIONARY rd.d (k UInt64, v String) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)",
            settings={"distributed_ddl_task_timeout": 0},
        )
        assert (
            node1.query(
                "SELECT count() FROM system.tables WHERE database = 'rd' AND name = 'd'"
            ).strip()
            == "0"
        )

        result = {}

        def rename():
            try:
                result["error"] = node1.query_and_get_error(
                    "RENAME TABLE rd.d TO rd.d_moved", user="u", password="p"
                )
            except Exception:
                result["error"] = "NO_ERROR"

        thread = threading.Thread(target=rename, daemon=True)
        thread.start()

        enqueued = node2.query_with_retry(
            f"SELECT count() FROM system.zookeeper WHERE path = '{ZK_PATH}/log' AND value LIKE '%RENAME TABLE%'",
            check_callback=lambda x: x.strip() == "1",
            retry_count=120,
        )
        assert enqueued.strip() == "1"

        node1.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        thread.join(30)
        assert not thread.is_alive()

        assert "ACCESS_DENIED" in result["error"]
        assert "DROP DICTIONARY" in result["error"]
        for node in (node1, node2):
            node.query("SYSTEM SYNC DATABASE REPLICA rd")
            assert (
                node.query(
                    "SELECT name, engine FROM system.tables WHERE database = 'rd' ORDER BY name"
                )
                == "d\tDictionary\n"
            )
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        for node in (node1, node2):
            node.query("DROP DATABASE IF EXISTS rd SYNC")
            node.query("DROP USER IF EXISTS u")
