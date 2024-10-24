import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/config_with_check_table_structure_completely.xml"]
)
# node1 = cluster.add_instance("node1")
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config_without_check_table_structure_completely.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# def test_setting_check_table_structure_completely(start_cluster):
#     assert node1.query("""select value from system.merge_tree_settings where name='enforce_index_structure_match_on_partition_manipulation';""") == "0\n"
def test_check_completely_attach_with_different_indices(start_cluster):
    node1.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node1.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number), toString(number) FROM numbers(10);"
    )
    node1.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE bloom_filter GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node1.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1,
            INDEX cf c TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node1.query("DROP TABLE attach_partition_t1")
    node1.query("DROP TABLE attach_partition_t2")
    node1.query("DROP TABLE attach_partition_t3")


def test_check_attach_with_different_indices(start_cluster):
    node2.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number), toString(number) FROM numbers(10);"
    )
    node2.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE bloom_filter GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node2.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            `c` String,
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1,
            INDEX cf c TYPE bloom_filter GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
    )
    assert node2.query("SELECT COUNT() FROM attach_partition_t3") == "10\n"
    assert node2.query("SELECT `a` FROM attach_partition_t3 WHERE `b` = '1'") == "1\n"
    assert node2.query("SELECT `a` FROM attach_partition_t3 WHERE `c` = '1'") == "1\n"
    node2.query("DROP TABLE attach_partition_t1")
    node2.query("DROP TABLE attach_partition_t2")
    node2.query("DROP TABLE attach_partition_t3")


def test_check_completely_attach_with_different_projections(start_cluster):
    node1.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node1.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number) FROM numbers(10);"
    )
    node1.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            PROJECTION differently_named_proj (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a;
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different projections" in str(exc.value)
    node1.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            PROJECTION proj2 (
                SELECT
                    b,
                    avg(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different projections" in str(exc.value)
    node1.query("DROP TABLE attach_partition_t1")
    node1.query("DROP TABLE attach_partition_t2")
    node1.query("DROP TABLE attach_partition_t3")


def test_check_attach_with_different_projections(start_cluster):
    node2.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number) FROM numbers(10);"
    )
    node2.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            PROJECTION differently_named_proj (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a;
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different projections" in str(exc.value)
    node2.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            PROJECTION proj2 (
                SELECT
                    b,
                    avg(a)
                GROUP BY b
            )
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
    )
    assert node2.query("SELECT COUNT() FROM attach_partition_t3") == "10\n"
    node2.query("DROP TABLE attach_partition_t1")
    node2.query("DROP TABLE attach_partition_t2")
    node2.query("DROP TABLE attach_partition_t3")


def test_check_completely_attach_with_different_indices_and_projections(start_cluster):
    node1.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node1.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number), toString(number) FROM numbers(10);"
    )
    node1.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            INDEX bf b TYPE bloom_filter GRANULARITY 1,
            INDEX cf c TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node1.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            PROJECTION proj2 (
                SELECT
                    b,
                    avg(a)
                GROUP BY b
            ),
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1,
            INDEX cf c TYPE bloom_filter GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node1.query("DROP TABLE attach_partition_t1")
    node1.query("DROP TABLE attach_partition_t2")
    node1.query("DROP TABLE attach_partition_t3")


def test_check_attach_with_different_indices_and_projections(start_cluster):
    node2.query(
        """
        CREATE TABLE attach_partition_t1
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "INSERT INTO attach_partition_t1 SELECT number, toString(number), toString(number) FROM numbers(10);"
    )
    node2.query(
        """
        CREATE TABLE attach_partition_t2
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            INDEX bf b TYPE bloom_filter GRANULARITY 1,
            INDEX cf c TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    # serverError 36
    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(
            "ALTER TABLE attach_partition_t2 ATTACH PARTITION tuple() FROM attach_partition_t1;"
        )
    assert "Tables have different secondary indices" in str(exc.value)
    node2.query(
        """
        CREATE TABLE attach_partition_t3
        (
            `a` UInt32,
            `b` String,
            `c` String,
            PROJECTION proj1 (
                SELECT
                    b,
                    sum(a)
                GROUP BY b
            ),
            PROJECTION proj2 (
                SELECT
                    b,
                    avg(a)
                GROUP BY b
            ),
            INDEX bf b TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1,
            INDEX cf c TYPE bloom_filter GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY a
        """
    )
    node2.query(
        "ALTER TABLE attach_partition_t3 ATTACH PARTITION tuple() FROM attach_partition_t1;"
    )
    assert node2.query("SELECT COUNT() FROM attach_partition_t3") == "10\n"
    assert node2.query("SELECT `a` FROM attach_partition_t3 WHERE `b` = '1'") == "1\n"
    assert node2.query("SELECT `a` FROM attach_partition_t3 WHERE `c` = '1'") == "1\n"
    node2.query("DROP TABLE attach_partition_t1")
    node2.query("DROP TABLE attach_partition_t2")
    node2.query("DROP TABLE attach_partition_t3")
