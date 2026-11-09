"""
An `IN` over a subquery that reads a Distributed table is pushed down to the shards. If asked to, they get the result of its right side. Here they must not
run the text: they have a local table with the name of the coordinator's Distributed one, or no such table at all.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

coordinator = cluster.add_instance("coordinator", main_configs=["configs/remote_servers.xml"])
shard1 = cluster.add_instance("shard1", main_configs=["configs/remote_servers.xml"])
shard2 = cluster.add_instance("shard2")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        # Events 1..10 live on shard1, 11..20 on shard2. The ids of `lookup` live on the other shard than their events.
        for node, events, lookup in (
            (shard1, "numbers(1, 10)", "numbers(11, 5)"),
            (shard2, "numbers(11, 10)", "numbers(1, 5)"),
        ):
            for table, rows in (("events", events), ("lookup", lookup)):
                node.query(f"CREATE TABLE {table} (id UInt64) ENGINE = MergeTree ORDER BY id")
                node.query(f"INSERT INTO {table} SELECT number FROM {rows}")

        for table in ("events", "lookup"):
            coordinator.query(f"CREATE TABLE {table} (id UInt64) ENGINE = Distributed(shards, default, {table})")
        coordinator.query("CREATE TABLE lookup_merge (id UInt64) ENGINE = Merge(default, '^lookup$')")
        coordinator.query("CREATE VIEW only_here AS SELECT id FROM lookup")

        # A local table that all have, with other rows, and one that only the coordinator has.
        for node, rows in ((coordinator, "numbers(1, 3)"), (shard1, "numbers(6, 3)"), (shard2, "numbers(16, 3)")):
            node.query("CREATE TABLE local (id UInt64) ENGINE = MergeTree ORDER BY id")
            node.query(f"INSERT INTO local SELECT number FROM {rows}")
        coordinator.query("CREATE TABLE local_here ENGINE = MergeTree ORDER BY id AS SELECT id FROM local")

        # A shard that is an initiator too, and sends its own table for the same `(SELECT id FROM lookup)`.
        shard1.query("CREATE TABLE all_events (id UInt64) ENGINE = Distributed(shards, default, events)")
        shard1.query("CREATE VIEW hop AS SELECT id FROM all_events WHERE id + 1 GLOBAL IN (SELECT id FROM lookup)")
        coordinator.query("CREATE TABLE hop (id UInt64) ENGINE = Distributed(first, default, hop)")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("mode,prefer_global", [("global", 0), ("deny", 1)])
def test_pushed_down_in(mode, prefer_global):
    events = "(SELECT id FROM events)"
    twice = "(SELECT id FROM events UNION ALL SELECT id FROM events)"
    for source, condition, expected in (
        (events, "id IN (SELECT id FROM lookup)", 10),
        (events, "id IN lookup", 10),
        (events, "id IN (SELECT id FROM only_here)", 10),
        (events, "id NOT IN (SELECT max(id) + 10 FROM lookup)", 20),
        (events, "id = 6 OR id IN (SELECT id FROM lookup)", 11),
        (events, "id IN (SELECT id FROM local)", 3),
        (events, "id IN local_here", 3),
        (events, "id NOT IN (SELECT id FROM lookup) OR id IN (SELECT id FROM local)", 13),
        (events, "id NOT IN (SELECT id FROM lookup) OR id IN (SELECT id FROM local WHERE id IN (SELECT id FROM lookup_merge))", 13),
        (events, "(id IN (SELECT id FROM lookup)) = (id IN (SELECT id FROM local))", 13),
        ("(SELECT id FROM hop)", "id IN (SELECT id FROM lookup)", 4),
        ("(SELECT id FROM hop)", "id + 1 IN (SELECT id FROM lookup)", 5),
        (twice, "id IN (SELECT id FROM only_here)", 20),
    ):
        result = coordinator.query(
            f"SELECT count() FROM {source} WHERE {condition}",
            settings={
                "enable_analyzer": 1,
                "distributed_product_mode": mode,
                "prefer_global_in_and_join": prefer_global,
            },
        )
        assert int(result) == expected, (source, condition)
