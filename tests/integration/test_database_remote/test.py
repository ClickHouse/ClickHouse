import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2")
node3 = cluster.add_instance("node3")
# A node with a configured host filter, to check which addresses the engine accepts.
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_hosts.xml", "configs/named_collections.xml"],
    user_configs=["configs/users_named_collections.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_replica_fallback(started_cluster):
    # The database exists only on node2. On node1 the replica that points to node1 itself is a local
    # shard, so the metadata lookup prefers the local catalog; when the local replica does not have
    # the database, the lookup must fall back to the remote replica, like the read path of the
    # `Distributed` storage does, instead of hiding the tables of the remote replica.
    node2.query("CREATE DATABASE fallback_src")
    node2.query(
        "CREATE TABLE fallback_src.t (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("INSERT INTO fallback_src.t VALUES (1), (2), (3)")

    node1.query(
        "CREATE DATABASE fallback_proxy ENGINE = Remote('node1|node2', 'fallback_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM fallback_proxy") == "t\n"
    assert node1.query("EXISTS TABLE fallback_proxy.t") == "1\n"
    description = node1.query("DESCRIBE TABLE fallback_proxy.t").split("\t")
    assert description[0] == "x"
    assert description[1].strip() == "UInt64"
    assert node1.query("SELECT count(), sum(x) FROM fallback_proxy.t") == "3\t6\n"

    node1.query("DROP DATABASE fallback_proxy")
    node2.query("DROP DATABASE fallback_src")


def test_local_only_shard_is_not_dropped_by_fallback(started_cluster):
    # `Remote('node1,node2', db)` on node1 describes two shards, and the first one consists of the
    # local replica only. When the database exists only on node2, there is no same-shard replica to
    # fall back to for the first shard; substituting a cluster without it would silently read and
    # write only node2, i.e. a subset of the configured shards. The table must be reported as
    # missing instead.
    node2.query("CREATE DATABASE mixed_src")
    node2.query("CREATE TABLE mixed_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO mixed_src.t VALUES (1)")

    node1.query(
        "CREATE DATABASE mixed_proxy ENGINE = Remote('node1,node2', 'mixed_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM mixed_proxy") == ""
    assert "UNKNOWN_TABLE" in node1.query_and_get_error("SELECT * FROM mixed_proxy.t")

    node1.query("DROP DATABASE mixed_proxy")
    node2.query("DROP DATABASE mixed_src")


def test_replica_fallback_needs_no_local_grants(started_cluster):
    # On the remote-replica fallback no local object is touched, so a user with rights on the proxy
    # database only (and none on the missing local counterpart of the remote database) must be able
    # to resolve and read the table.
    node2.query("CREATE DATABASE grants_src")
    node2.query("CREATE TABLE grants_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO grants_src.t VALUES (7)")

    node1.query(
        "CREATE DATABASE grants_proxy ENGINE = Remote('node1|node2', 'grants_src', 'default', '')"
    )
    node1.query("CREATE USER restricted_user IDENTIFIED WITH no_password")
    node1.query("GRANT SHOW, SELECT ON grants_proxy.* TO restricted_user")

    assert (
        node1.query("SELECT x FROM grants_proxy.t", user="restricted_user") == "7\n"
    )

    node1.query("DROP USER restricted_user")
    node1.query("DROP DATABASE grants_proxy")
    node2.query("DROP DATABASE grants_src")


def test_listing_includes_a_table_of_a_remote_replica(started_cluster):
    # The database exists on the local replica of the shard, but one of its tables exists only on the
    # remote replica. Resolution falls back to that replica, so the listing has to include the table
    # as well: otherwise the same name would be missing from `SHOW TABLES` and `system.tables` while
    # `EXISTS TABLE`, `DESCRIBE` and `SELECT` answer for it.
    for node in (node1, node2):
        node.query("CREATE DATABASE listing_src")
        node.query("CREATE TABLE listing_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query(
        "CREATE TABLE listing_src.only_remote (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("INSERT INTO listing_src.only_remote VALUES (5)")

    node1.query(
        "CREATE DATABASE listing_proxy ENGINE = Remote('node1|node2', 'listing_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM listing_proxy") == "both\nonly_remote\n"
    assert (
        node1.query(
            "SELECT name FROM system.tables WHERE database = 'listing_proxy' ORDER BY name"
        )
        == "both\nonly_remote\n"
    )
    assert node1.query("EXISTS TABLE listing_proxy.only_remote") == "1\n"
    assert node1.query("SELECT sum(x) FROM listing_proxy.only_remote") == "5\n"

    node1.query("DROP DATABASE listing_proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE listing_src")


def test_listing_does_not_fail_when_a_remote_replica_is_unavailable(started_cluster):
    # The listing is completed from the remote replicas of the shard, but they are consulted only to
    # add what the local replica does not have: when none of them answers, the list of the local
    # replica is still a valid answer of an available replica, so `SHOW TABLES` must not fail.
    node1.query("CREATE DATABASE unavailable_src")
    node1.query("CREATE TABLE unavailable_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query(
        "CREATE DATABASE unavailable_proxy ENGINE = Remote('node1|127.0.0.1:1', 'unavailable_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM unavailable_proxy") == "t\n"

    node1.query("DROP DATABASE unavailable_proxy")
    node1.query("DROP DATABASE unavailable_src")


def test_multi_shard_metadata_comes_from_one_shard(started_cluster):
    # `Remote('node1,node2', db)` on node3 describes two remote shards. The metadata is resolved from
    # an arbitrary shard -- the first reachable one, here node1 -- so that a listing costs a single
    # query instead of one per shard, exactly like the `remote` table function resolves the structure.
    # A table present on both shards is served, aggregating over both; `only_on_node2`, which the
    # resolving shard does not have, is not exposed.
    node1.query("CREATE DATABASE sharded_src")
    node1.query("CREATE TABLE sharded_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO sharded_src.both VALUES (1)")
    node2.query("CREATE DATABASE sharded_src")
    node2.query("CREATE TABLE sharded_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO sharded_src.both VALUES (2)")
    node2.query(
        "CREATE TABLE sharded_src.only_on_node2 (x UInt64) ENGINE = MergeTree ORDER BY x"
    )

    node3.query(
        "CREATE DATABASE sharded_proxy ENGINE = Remote('node1,node2', 'sharded_src', 'default', '')"
    )

    assert node3.query("SHOW TABLES FROM sharded_proxy") == "both\n"
    assert node3.query("EXISTS TABLE sharded_proxy.both") == "1\n"
    assert node3.query("EXISTS TABLE sharded_proxy.only_on_node2") == "0\n"
    assert node3.query("SELECT count(), sum(x) FROM sharded_proxy.both") == "2\t3\n"
    # Reading a table that only one shard has fails on the shard that does not have it (the structure
    # itself is inferred from whichever shard can describe it, like `getStructureOfRemoteTable` does).
    assert "There is no table" in node3.query_and_get_error(
        "SELECT * FROM sharded_proxy.only_on_node2"
    )

    node3.query("DROP DATABASE sharded_proxy")
    node1.query("DROP DATABASE sharded_src")
    node2.query("DROP DATABASE sharded_src")


def test_multi_shard_metadata_from_the_local_shard(started_cluster):
    # The symmetric case: `Remote('node1,node2', db)` on node1 resolves the metadata through the local
    # catalog of the local shard, without a round trip. The shards are expected to serve the same set
    # of tables: a table that only the resolving shard has is still exposed, and reading it fails on
    # the shard that does not have it.
    node1.query("CREATE DATABASE partial_src")
    node1.query(
        "CREATE TABLE partial_src.only_local (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node1.query("CREATE TABLE partial_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO partial_src.both VALUES (10)")
    node2.query("CREATE DATABASE partial_src")
    node2.query("CREATE TABLE partial_src.both (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO partial_src.both VALUES (20)")

    node1.query(
        "CREATE DATABASE partial_proxy ENGINE = Remote('node1,node2', 'partial_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM partial_proxy") == "both\nonly_local\n"
    assert node1.query("EXISTS TABLE partial_proxy.only_local") == "1\n"
    assert "There is no table" in node1.query_and_get_error(
        "SELECT * FROM partial_proxy.only_local"
    )
    assert node1.query("SELECT count(), sum(x) FROM partial_proxy.both") == "2\t30\n"

    node1.query("DROP DATABASE partial_proxy")
    node1.query("DROP DATABASE partial_src")
    node2.query("DROP DATABASE partial_src")


def test_insert_into_a_multi_shard_database(started_cluster):
    # A proxy table of a multi-shard database carries an implicit `rand()` sharding key, so it is
    # writable by default: each row of an `INSERT` goes to a random shard. Without the key,
    # `StorageDistributed::write` would reject the `INSERT` with `STORAGE_REQUIRES_PARAMETER`
    # unless the caller set `insert_shard_id` or `insert_distributed_one_random_shard`.
    for node in (node1, node2):
        node.query("CREATE DATABASE ins_src")
        node.query("CREATE TABLE ins_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")

    node3.query(
        "CREATE DATABASE ins_proxy ENGINE = Remote('node1,node2', 'ins_src', 'default', '')"
    )

    node3.query("INSERT INTO ins_proxy.t SELECT number FROM numbers(100)")
    assert node3.query("SELECT count(), sum(x) FROM ins_proxy.t") == "100\t4950\n"
    # Every row landed on exactly one of the shards.
    assert (
        int(node1.query("SELECT count() FROM ins_src.t"))
        + int(node2.query("SELECT count() FROM ins_src.t"))
        == 100
    )

    # An explicit `insert_shard_id` still pins the shard for a query.
    node3.query(
        "INSERT INTO ins_proxy.t VALUES (1000)", settings={"insert_shard_id": 2}
    )
    assert node2.query("SELECT count() FROM ins_src.t WHERE x = 1000") == "1\n"

    node3.query("DROP DATABASE ins_proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE ins_src")


def test_local_replica_preferred(started_cluster):
    # When the local replica does have the database, the metadata comes from the local catalog
    # without a self-connection, and queries read the local replica.
    node1.query("CREATE DATABASE local_src")
    node1.query("CREATE TABLE local_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node1.query("INSERT INTO local_src.t VALUES (1), (2)")

    node1.query(
        "CREATE DATABASE local_proxy ENGINE = Remote('node1|node2', 'local_src', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM local_proxy") == "t\n"
    assert node1.query("SELECT count(), sum(x) FROM local_proxy.t") == "2\t3\n"

    node1.query("DROP DATABASE local_proxy")
    node1.query("DROP DATABASE local_src")


def test_show_create_table_serializes_fallback_addresses(started_cluster):
    # When the local replica does not have the database, the live proxy is bound to the remote-only
    # fallback cluster. `SHOW CREATE TABLE` must serialize those effective addresses: a definition
    # replayed with the configured 'node1|node2' addresses would recreate a `Remote` table that
    # fails on the missing local database instead of reaching the fallback.
    node2.query("CREATE DATABASE sc_src")
    node2.query("CREATE TABLE sc_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query("INSERT INTO sc_src.t VALUES (1), (2)")

    node1.query("CREATE DATABASE sc_proxy ENGINE = Remote('node1|node2', 'sc_src')")

    create_query = node1.query("SHOW CREATE TABLE sc_proxy.t FORMAT TSVRaw").strip()
    assert "Remote('node2:9000', 'sc_src', 't')" in create_query
    assert "node1" not in create_query

    # The printed definition must reconstruct an equivalent, working table.
    node1.query(
        create_query.replace(
            "CREATE TABLE sc_proxy.t", "CREATE TABLE default.sc_replayed"
        )
    )
    assert node1.query("SELECT count(), sum(x) FROM default.sc_replayed") == "2\t3\n"
    node1.query(
        "INSERT INTO default.sc_replayed VALUES (3)",
        settings={"distributed_foreground_insert": 1},
    )
    assert node1.query("SELECT count(), sum(x) FROM sc_proxy.t") == "3\t6\n"

    node1.query("DROP TABLE default.sc_replayed")
    node1.query("DROP DATABASE sc_proxy")
    node2.query("DROP DATABASE sc_src")


def test_host_filter_understands_ipv6_addresses(started_cluster):
    # `remote_url_allow_hosts` is configured on node4, so every address of the engine is checked
    # against it. A bracketed IPv6 literal has to be split at the closing bracket: splitting it at
    # the first `:` would check the host `[` and make a perfectly allowed address unusable.
    # No connection is attempted by `CREATE DATABASE`, so the address does not have to be reachable.
    node4.query(
        "CREATE DATABASE ipv6_allowed ENGINE = Remote('[2001:db8::1]:9000', 'default')"
    )
    node4.query("DROP DATABASE ipv6_allowed")

    # A host that is not in the list is still rejected.
    assert "UNACCEPTABLE_URL" in node4.query_and_get_error(
        "CREATE DATABASE ipv6_denied ENGINE = Remote('[2001:db8::2]:9000', 'default')"
    )
    assert "UNACCEPTABLE_URL" in node4.query_and_get_error(
        "CREATE DATABASE denied ENGINE = Remote('node3', 'default')"
    )

    # A plain host name keeps working.
    node4.query("CREATE DATABASE allowed ENGINE = Remote('node2', 'default')")
    node4.query("DROP DATABASE allowed")


def test_named_collection_supports_ipv6_host_and_port(started_cluster):
    # A named collection may supply the address as separate `host` / `port` keys. An IPv6 literal
    # has to be bracketed before the port is appended: a naive concatenation would produce
    # `2001:db8::1:9000`, which `parseAddress` rejects, so an address that works positionally as
    # `Remote('[2001:db8::1]:9000', ...)` would be unusable through the equivalent collection.
    # The collection `remote_ipv6_collection` is defined in configs/named_collections.xml, and the
    # host is in node4's `remote_url_allow_hosts`, so the definition must be accepted (no
    # connection is attempted by `CREATE DATABASE`).
    node4.query(
        "CREATE DATABASE nc_ipv6 ENGINE = Remote(remote_ipv6_collection)"
    )
    node4.query("DROP DATABASE nc_ipv6")

    # The host filter sees the bare IPv6 host through the named-collection path as well: a
    # collection pointing to a host that is not in the list is rejected, not misparsed.
    assert "UNACCEPTABLE_URL" in node4.query_and_get_error(
        "CREATE DATABASE nc_ipv6_denied ENGINE = Remote(remote_ipv6_denied_collection)"
    )


def test_hidden_local_table_is_not_served_by_a_remote_replica(started_cluster):
    # The remote-replica fallback must not become a way around the visibility rule of the local shard.
    # `db.t` exists on node1 (the local replica of the shard) and on node2 (its remote replica), and
    # the caller has rights on the proxy database only, so the name is hidden. It must stay hidden for
    # resolution as well: the fallback exists for a table the local replica genuinely lacks, not for a
    # table it is not allowed to expose.
    for node in (node1, node2):
        node.query("CREATE DATABASE hidden_src")
        node.query("CREATE TABLE hidden_src.t (x UInt64) ENGINE = MergeTree ORDER BY x")
        node.query("INSERT INTO hidden_src.t VALUES (1)")

    node1.query(
        "CREATE DATABASE hidden_proxy ENGINE = Remote('node1|node2', 'hidden_src', 'default', '')"
    )
    node1.query("CREATE USER hidden_user IDENTIFIED WITH no_password")
    node1.query("GRANT SHOW, SELECT, INSERT ON hidden_proxy.* TO hidden_user")

    assert node1.query("SHOW TABLES FROM hidden_proxy", user="hidden_user") == ""
    assert node1.query("EXISTS TABLE hidden_proxy.t", user="hidden_user") == "0\n"
    for query in (
        "DESCRIBE TABLE hidden_proxy.t",
        "SHOW CREATE TABLE hidden_proxy.t",
        "SELECT * FROM hidden_proxy.t",
        "INSERT INTO hidden_proxy.t VALUES (2)",
    ):
        assert "UNKNOWN_TABLE" in node1.query_and_get_error(query, user="hidden_user")

    # The owner of the underlying tables still sees and reads them.
    assert node1.query("SHOW TABLES FROM hidden_proxy") == "t\n"
    assert node1.query("SELECT sum(x) FROM hidden_proxy.t") == "1\n"

    node1.query("DROP USER hidden_user")
    node1.query("DROP DATABASE hidden_proxy")
    for node in (node1, node2):
        node.query("DROP DATABASE hidden_src")


def test_hidden_table_of_a_nested_remote_database_is_not_served_by_a_remote_replica(
    started_cluster,
):
    # The same rule has to survive one more level: the local shard of the outer proxy is itself a
    # `Remote` database, which hides `t` from the caller. The outer proxy must not resolve that very
    # table through the other replica of its shard under the stored engine credentials.
    for node in (node1, node2):
        node.query("CREATE DATABASE nested_hidden_src")
        node.query(
            "CREATE TABLE nested_hidden_src.t (x UInt64) ENGINE = MergeTree ORDER BY x"
        )
        node.query("INSERT INTO nested_hidden_src.t VALUES (1)")
        node.query(
            "CREATE DATABASE nested_hidden_inner ENGINE = Remote('127.0.0.1', 'nested_hidden_src', 'default', '')"
        )

    node1.query(
        "CREATE DATABASE nested_hidden_outer ENGINE = Remote('node1|node2', 'nested_hidden_inner', 'default', '')"
    )
    node1.query("CREATE USER nested_hidden_user IDENTIFIED WITH no_password")
    node1.query(
        "GRANT SHOW, SELECT, INSERT ON nested_hidden_outer.* TO nested_hidden_user"
    )

    assert (
        node1.query("SHOW TABLES FROM nested_hidden_outer", user="nested_hidden_user")
        == ""
    )
    assert (
        node1.query(
            "EXISTS TABLE nested_hidden_outer.t", user="nested_hidden_user"
        )
        == "0\n"
    )
    for query in (
        "DESCRIBE TABLE nested_hidden_outer.t",
        "SHOW CREATE TABLE nested_hidden_outer.t",
        "SELECT * FROM nested_hidden_outer.t",
        "INSERT INTO nested_hidden_outer.t VALUES (2)",
    ):
        assert "UNKNOWN_TABLE" in node1.query_and_get_error(
            query, user="nested_hidden_user"
        )

    # A caller that is allowed to see the innermost table still works through the whole chain,
    # without any grants on the intermediate database.
    node1.query(
        "GRANT SHOW, SELECT ON nested_hidden_src.* TO nested_hidden_user"
    )
    assert (
        node1.query("SHOW TABLES FROM nested_hidden_outer", user="nested_hidden_user")
        == "t\n"
    )
    assert (
        node1.query(
            "EXISTS TABLE nested_hidden_outer.t", user="nested_hidden_user"
        )
        == "1\n"
    )
    assert (
        node1.query(
            "DESCRIBE TABLE nested_hidden_outer.t", user="nested_hidden_user"
        ).startswith("x\tUInt64")
    )

    node1.query("DROP USER nested_hidden_user")
    node1.query("DROP DATABASE nested_hidden_outer")
    for node in (node1, node2):
        node.query("DROP DATABASE nested_hidden_inner")
        node.query("DROP DATABASE nested_hidden_src")


def test_unavailable_nested_remote_database_falls_back_to_a_healthy_replica(
    started_cluster,
):
    # The local shard of the outer proxy is itself a `Remote` database whose own target is down. That
    # failure means the local replica of the outer shard cannot answer, exactly as if the replica
    # itself were unavailable, so the outer proxy must fall back to the other replica of the same
    # shard (where the intermediate database resolves the table locally) instead of surfacing the
    # error of the broken intermediate proxy.
    node2.query("CREATE DATABASE nested_down_src")
    node2.query(
        "CREATE TABLE nested_down_src.t (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    node2.query("INSERT INTO nested_down_src.t VALUES (1), (2), (3)")
    node2.query(
        "CREATE DATABASE nested_down_inner ENGINE = Remote('127.0.0.1', 'nested_down_src', 'default', '')"
    )
    # The intermediate database on node1 points at an unreachable address.
    node1.query(
        "CREATE DATABASE nested_down_inner ENGINE = Remote('127.0.0.1:1', 'nested_down_src', 'default', '')"
    )
    node1.query(
        "CREATE DATABASE nested_down_outer ENGINE = Remote('node1|node2', 'nested_down_inner', 'default', '')"
    )

    assert node1.query("SHOW TABLES FROM nested_down_outer") == "t\n"
    assert node1.query("EXISTS TABLE nested_down_outer.t") == "1\n"
    assert node1.query("DESCRIBE TABLE nested_down_outer.t").startswith("x\tUInt64")
    assert node1.query("SELECT count(), sum(x) FROM nested_down_outer.t") == "3\t6\n"

    node1.query("DROP DATABASE nested_down_outer")
    for node in (node1, node2):
        node.query("DROP DATABASE nested_down_inner")
    node2.query("DROP DATABASE nested_down_src")
