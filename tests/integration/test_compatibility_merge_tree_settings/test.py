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
        CREATE TABLE tp (type Int32, eventcnt UInt64, PROJECTION p (select sum(eventcnt), type group by type))
        engine = {} order by type;
    """

    create_no_projection = """
        CREATE TABLE tp (type Int32, eventcnt UInt64)
        engine = {} order by type;
    """

    alter_add_projection = """
        ALTER TABLE tp ADD PROJECTION p (select sum(eventcnt), type group by type);
    """

    # Create with invalid projection is not supported by default

    assert "Projection is fully supported" in node.query_and_get_error(
        create_with_invalid_projection.format("ReplacingMergeTree")
    )
    assert "Projection is fully supported" in node.query_and_get_error(
        create_with_invalid_projection.format(
            "ReplicatedReplacingMergeTree('/tables/tp', '0')"
        )
    )

    # Adding invalid projection is not supported by default

    node.query(create_no_projection.format("ReplacingMergeTree"))
    assert "Projection is fully supported" in node.query_and_get_error(
        alter_add_projection
    )
    node.query("drop table tp;")

    node.query(
        create_no_projection.format("ReplicatedReplacingMergeTree('/tables/tp', '0')")
    )
    assert "Projection is fully supported" in node.query_and_get_error(
        alter_add_projection
    )
    node.query("drop table tp;")

    # Create with invalid projection is supported with compatibility

    node_with_compatibility.query(
        create_with_invalid_projection.format("ReplacingMergeTree")
    )
    node_with_compatibility.query("drop table tp;")
    node_with_compatibility.query(
        create_with_invalid_projection.format(
            "ReplicatedReplacingMergeTree('/tables/tp2', '0')"
        )
    )
    node_with_compatibility.query("drop table tp;")

    # Adding invalid projection is supported with compatibility

    node_with_compatibility.query(create_no_projection.format("ReplacingMergeTree"))
    node_with_compatibility.query(alter_add_projection)
    node_with_compatibility.query("drop table tp;")

    node_with_compatibility.query(
        create_no_projection.format("ReplicatedReplacingMergeTree('/tables/tp3', '0')")
    )
    node_with_compatibility.query(alter_add_projection)
    node_with_compatibility.query("drop table tp;")


def test_config_overrides_compatibility(started_cluster):
    create_with_invalid_projection = """
        CREATE TABLE tp (type Int32, eventcnt UInt64, PROJECTION p (select sum(eventcnt), type group by type))
        engine = {} order by type;
    """

    assert (
        "Projection is fully supported"
        in node_with_compatibility_and_mt_setings.query_and_get_error(
            create_with_invalid_projection.format("ReplacingMergeTree")
        )
    )
    assert (
        "Projection is fully supported"
        in node_with_compatibility_and_mt_setings.query_and_get_error(
            create_with_invalid_projection.format(
                "ReplicatedReplacingMergeTree('/tables/tp', '0')"
            )
        )
    )
