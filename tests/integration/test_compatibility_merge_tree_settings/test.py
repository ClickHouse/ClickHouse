import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node1", with_zookeeper=True)
node_with_compatibility = cluster.add_instance(
    "node2", with_zookeeper=True, user_configs=["configs/compatibility.xml"]
)
node_with_compatibility_and_mt_setings = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    main_configs=["configs/mt_settings.xml"],
    user_configs=["configs/compatibility.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_check_projections_compatibility(started_cluster):
    create_with_invalid_projection = """
        CREATE TABLE tp (type Int32, eventcnt UInt64, PROJECTION p (SELECT sum(eventcnt), type GROUP BY type))
        ENGINE = {} ORDER BY type;
    """

    create_without_projection = """
        CREATE TABLE tp (type Int32, eventcnt UInt64)
        ENGINE = {} ORDER BY type;
    """

    alter_add_projection = """
        ALTER TABLE tp ADD PROJECTION p (SELECT sum(eventcnt), type GROUP BY type);
    """

    # Create with invalid projection is not supported by default

    assert "Projections are not supported" in node.query_and_get_error(
        create_with_invalid_projection.format("ReplacingMergeTree")
    )
    assert "Projections are not supported" in node.query_and_get_error(
        create_with_invalid_projection.format(
            "ReplicatedReplacingMergeTree('/tables/tp', '0')"
        )
    )

    # Adding invalid projection is not supported by default

    node.query(create_without_projection.format("ReplacingMergeTree"))
    assert "ADD PROJECTION is not supported" in node.query_and_get_error(
        alter_add_projection
    )
    node.query("DROP TABLE tp SYNC")

    node.query(
        create_without_projection.format("ReplicatedReplacingMergeTree('/tables/tp', '0')")
    )
    assert "ADD PROJECTION is not supported" in node.query_and_get_error(
        alter_add_projection
    )
    node.query("DROP TABLE tp SYNC")

    # Create with invalid projection is supported with compatibility

    node_with_compatibility.query(
        create_with_invalid_projection.format("ReplacingMergeTree")
    )
    node_with_compatibility.query("DROP TABLE tp SYNC")
    node_with_compatibility.query(
        create_with_invalid_projection.format(
            "ReplicatedReplacingMergeTree('/tables/tp2', '0')"
        )
    )
    node_with_compatibility.query("DROP TABLE tp SYNC")

    # Adding invalid projection is supported with compatibility

    node_with_compatibility.query(create_without_projection.format("ReplacingMergeTree"))
    node_with_compatibility.query(alter_add_projection)
    node_with_compatibility.query("DROP TABLE tp SYNC")

    node_with_compatibility.query(
        create_without_projection.format("ReplicatedReplacingMergeTree('/tables/tp3', '0')")
    )
    node_with_compatibility.query(alter_add_projection)
    node_with_compatibility.query("DROP TABLE tp SYNC")


def test_config_overrides_compatibility(started_cluster):
    create_with_invalid_projection = """
        CREATE TABLE tp (type Int32, eventcnt UInt64, PROJECTION p (select sum(eventcnt), type group by type))
        engine = {} order by type;
    """

    assert (
        "Projections are not supported"
        in node_with_compatibility_and_mt_setings.query_and_get_error(
            create_with_invalid_projection.format("ReplacingMergeTree")
        )
    )
    assert (
        "Projections are not supported"
        in node_with_compatibility_and_mt_setings.query_and_get_error(
            create_with_invalid_projection.format(
                "ReplicatedReplacingMergeTree('/tables/tp', '0')"
            )
        )
    )


# Rows whose optimized row order differs from their insertion order (same data as
# 04146_merge_tree_optimize_row_order_if_no_order_by).
ROW_ORDER_ROWS = (
    "('Bob', 4, 100, '1'), ('Nikita', 2, 54, '1'), ('Nikita', 1, 228, '1'), "
    "('Alex', 4, 83, '1'), ('Alex', 4, 134, '1'), ('Alex', 1, 65, '0'), "
    "('Alex', 4, 134, '1'), ('Bob', 2, 53, '0'), ('Alex', 4, 83, '0'), "
    "('Alex', 1, 63, '1'), ('Bob', 2, 53, '1'), ('Alex', 4, 192, '1'), "
    "('Alex', 2, 128, '1'), ('Nikita', 2, 148, '0'), ('Bob', 4, 177, '0'), "
    "('Nikita', 1, 173, '0'), ('Alex', 1, 239, '0'), ('Alex', 1, 63, '0'), "
    "('Alex', 2, 224, '1'), ('Bob', 4, 177, '0'), ('Alex', 2, 128, '1'), "
    "('Alex', 4, 134, '0'), ('Alex', 4, 83, '1'), ('Bob', 4, 100, '0'), "
    "('Nikita', 2, 54, '1'), ('Alex', 1, 239, '1'), ('Bob', 2, 187, '1'), "
    "('Alex', 1, 65, '1'), ('Bob', 2, 53, '1'), ('Alex', 2, 224, '0'), "
    "('Alex', 4, 192, '0'), ('Nikita', 1, 173, '1'), ('Nikita', 2, 148, '1'), "
    "('Bob', 2, 187, '1'), ('Nikita', 2, 208, '1'), ('Nikita', 2, 208, '0'), "
    "('Nikita', 1, 228, '0'), ('Nikita', 2, 148, '0')"
)


def _on_disk_order(instance, table):
    return instance.query(
        f"SELECT groupArray((name, timestamp, money, flag)) "
        f"FROM (SELECT * FROM {table}) SETTINGS max_threads = 1"
    )


def _create_row_order_table(instance, table, extra_settings=""):
    settings = "add_minmax_index_for_numeric_columns = 0"
    if extra_settings:
        settings = f"{extra_settings}, {settings}"
    instance.query(f"DROP TABLE IF EXISTS {table} SYNC")
    instance.query(
        f"""
        CREATE TABLE {table} (
            name String,
            timestamp Int64,
            money UInt8,
            flag String
        ) ENGINE = MergeTree
        ORDER BY ()
        SETTINGS {settings}
        """
    )
    instance.query(
        f"INSERT INTO {table} VALUES {ROW_ORDER_ROWS}",
        settings={"max_insert_threads": 1},
    )


def test_optimize_row_order_if_no_order_by_compatibility(started_cluster):
    # Under an old `compatibility` setting, `optimize_row_order_if_no_order_by` must
    # default to its previous value (off), so the on-disk order of an `ORDER BY ()`
    # table with no explicit setting matches the insertion order.
    _create_row_order_table(node_with_compatibility, "t_compat_default")
    _create_row_order_table(
        node_with_compatibility, "t_compat_off", "optimize_row_order_if_no_order_by = 0"
    )
    _create_row_order_table(
        node_with_compatibility, "t_compat_on", "optimize_row_order_if_no_order_by = 1"
    )

    # With the old compatibility the default behaves like the disabled setting ...
    assert _on_disk_order(node_with_compatibility, "t_compat_default") == _on_disk_order(
        node_with_compatibility, "t_compat_off"
    )
    # ... and differs from the explicitly enabled optimization.
    assert _on_disk_order(node_with_compatibility, "t_compat_default") != _on_disk_order(
        node_with_compatibility, "t_compat_on"
    )

    for table in ("t_compat_default", "t_compat_off", "t_compat_on"):
        node_with_compatibility.query(f"DROP TABLE {table} SYNC")

    # Without any compatibility, the default is on, so the same table optimizes the
    # row order (matching the explicitly enabled setting, differing from disabled).
    _create_row_order_table(node, "t_default")
    _create_row_order_table(node, "t_off", "optimize_row_order_if_no_order_by = 0")
    _create_row_order_table(node, "t_on", "optimize_row_order_if_no_order_by = 1")

    assert _on_disk_order(node, "t_default") == _on_disk_order(node, "t_on")
    assert _on_disk_order(node, "t_default") != _on_disk_order(node, "t_off")

    for table in ("t_default", "t_off", "t_on"):
        node.query(f"DROP TABLE {table} SYNC")
