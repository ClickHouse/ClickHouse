import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query("SET allow_experimental_hybrid_table = 1")
            node.query(
                "CREATE TABLE local_hot (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts"
            )
            node.query(
                "CREATE TABLE local_cold (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts"
            )

        node1.query(
            "INSERT INTO local_hot VALUES ('2025-10-15', 1), ('2025-11-01', 2)"
        )
        node1.query(
            "INSERT INTO local_cold VALUES ('2025-08-01', 3), ('2025-06-15', 4)"
        )
        node2.query(
            "INSERT INTO local_hot VALUES ('2025-10-20', 10), ('2025-12-01', 20)"
        )
        node2.query(
            "INSERT INTO local_cold VALUES ('2025-07-01', 30), ('2025-05-01', 40)"
        )

        yield cluster
    finally:
        cluster.shutdown()


def q(node, sql):
    return node.query(sql, settings={"allow_experimental_hybrid_table": 1}).strip()


def test_create_and_alter_watermark(started_cluster):
    """Create a Hybrid table, verify routing, ALTER the watermark, verify again."""
    try:
        q(
            node1,
            """
            CREATE TABLE hybrid_t
            ENGINE = Hybrid(
                remote('node1:9000', default, local_hot),
                    ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
                remote('node1:9000', default, local_cold),
                    ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
            )
            SETTINGS hybrid_watermark_hot = '2025-09-01'
            AS local_hot
            """,
        )

        assert q(node1, "SELECT count() FROM hybrid_t") == "4"
        assert q(node1, "SELECT count() FROM hybrid_t WHERE ts = '2025-10-15'") == "1"
        assert q(node1, "SELECT count() FROM hybrid_t WHERE ts = '2025-08-01'") == "1"

        show = q(node1, "SHOW CREATE TABLE hybrid_t")
        assert "hybrid_watermark_hot = \\'2025-09-01\\'" in show

        q(node1, "ALTER TABLE hybrid_t MODIFY SETTING hybrid_watermark_hot = '2025-10-01'")

        assert q(node1, "SELECT count() FROM hybrid_t WHERE ts = '2025-10-15'") == "1"
        assert q(node1, "SELECT count() FROM hybrid_t WHERE ts = '2025-08-01'") == "1"

        show = q(node1, "SHOW CREATE TABLE hybrid_t")
        assert "hybrid_watermark_hot = \\'2025-10-01\\'" in show
    finally:
        q(node1, "DROP TABLE IF EXISTS hybrid_t SYNC")


def test_detach_attach_persistence(started_cluster):
    """Watermark value survives DETACH/ATTACH cycle."""
    try:
        q(
            node1,
            """
            CREATE TABLE hybrid_persist
            ENGINE = Hybrid(
                remote('node1:9000', default, local_hot),
                    ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
                remote('node1:9000', default, local_cold),
                    ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
            )
            SETTINGS hybrid_watermark_hot = '2025-09-01'
            AS local_hot
            """,
        )

        q(node1, "ALTER TABLE hybrid_persist MODIFY SETTING hybrid_watermark_hot = '2025-10-01'")

        q(node1, "DETACH TABLE hybrid_persist")
        q(node1, "ATTACH TABLE hybrid_persist")

        show = q(node1, "SHOW CREATE TABLE hybrid_persist")
        assert "hybrid_watermark_hot = \\'2025-10-01\\'" in show

        assert q(node1, "SELECT count() FROM hybrid_persist") == "4"
    finally:
        q(node1, "DROP TABLE IF EXISTS hybrid_persist SYNC")


def test_cross_node_create_and_alter(started_cluster):
    """Hybrid table spanning two physical nodes via remote(), with ALTER."""
    try:
        q(
            node1,
            """
            CREATE TABLE hybrid_cross
            ENGINE = Hybrid(
                remote('node1:9000', default, local_hot),
                    ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
                remote('node2:9000', default, local_cold),
                    ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
            )
            SETTINGS hybrid_watermark_hot = '2025-09-01'
            AS local_hot
            """,
        )

        assert q(node1, "SELECT count() FROM hybrid_cross WHERE ts > '2025-09-01'") == "2"
        assert q(node1, "SELECT count() FROM hybrid_cross WHERE ts <= '2025-09-01'") == "2"
        assert q(node1, "SELECT count() FROM hybrid_cross") == "4"

        q(node1, "ALTER TABLE hybrid_cross MODIFY SETTING hybrid_watermark_hot = '2025-06-01'")

        assert q(node1, "SELECT count() FROM hybrid_cross WHERE ts > '2025-06-01'") == "2"
        assert q(node1, "SELECT count() FROM hybrid_cross WHERE ts <= '2025-06-01'") == "1"
    finally:
        q(node1, "DROP TABLE IF EXISTS hybrid_cross SYNC")


def test_cluster_table_function(started_cluster):
    """Hybrid using cluster() table function instead of remote()."""
    try:
        q(
            node1,
            """
            CREATE TABLE hybrid_cluster
            ENGINE = Hybrid(
                cluster('test_cluster', default, local_hot),
                    ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
                cluster('test_cluster', default, local_cold),
                    ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
            )
            SETTINGS hybrid_watermark_hot = '2025-09-01'
            AS local_hot
            """,
        )

        total = q(node1, "SELECT count() FROM hybrid_cluster")
        assert int(total) == 8

        q(node1, "ALTER TABLE hybrid_cluster MODIFY SETTING hybrid_watermark_hot = '2025-10-01'")

        hot = q(node1, "SELECT count() FROM hybrid_cluster WHERE ts > '2025-10-01'")
        assert int(hot) > 0
    finally:
        q(node1, "DROP TABLE IF EXISTS hybrid_cluster SYNC")
