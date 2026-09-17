"""Real-server semantic-boundary and query-cache correctness coverage."""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/udt.xml"],
    stay_alive=True,
    with_remote_database_disk=False,
)

ENABLED = {
    "allow_experimental_analyzer": 1,
    "allow_experimental_user_defined_types": 1,
}
CACHE_ENABLED = {
    **ENABLED,
    "use_query_cache": 1,
    "query_cache_min_query_runs": 0,
    "query_cache_ttl": 600,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def q(sql, *, settings=ENABLED):
    return node.query(sql, settings=settings)


def unique_database(prefix):
    return f"udt_semantic_boundary_{prefix}_{uuid.uuid4().hex[:8]}"


def cache_events():
    rows = q(
        "SELECT event, value FROM system.events "
        "WHERE event IN ('QueryCacheHits', 'QueryCacheMisses') FORMAT TSV"
    ).splitlines()
    result = {"QueryCacheHits": 0, "QueryCacheMisses": 0}
    result.update({name: int(value) for name, value in (row.split("\t") for row in rows)})
    return result


def assert_cache_miss(query, expected):
    before = cache_events()
    assert q(query, settings=CACHE_ENABLED).strip() == expected
    after = cache_events()
    assert after["QueryCacheHits"] == before["QueryCacheHits"], (before, after)
    assert after["QueryCacheMisses"] == before["QueryCacheMisses"] + 1, (
        before,
        after,
    )


def assert_cache_hit(query, expected):
    before = cache_events()
    assert q(query, settings=CACHE_ENABLED).strip() == expected
    after = cache_events()
    assert after["QueryCacheHits"] == before["QueryCacheHits"] + 1, (before, after)
    assert after["QueryCacheMisses"] == before["QueryCacheMisses"], (before, after)


def test_real_analyzer_join_union_default_dag_and_recursive_cte(started_cluster):
    database = unique_database("analyzer")
    try:
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.AccountId AS UInt64")
        q(f"CREATE TYPE {database}.Box(T TYPE) AS Tuple(value T)")
        q(
            f"CREATE TABLE {database}.left_values ("
            f"id {database}.UserId, boxed {database}.Box({database}.UserId), "
            f"ids Array({database}.UserId)) ENGINE = Memory"
        )
        q(
            f"CREATE TABLE {database}.right_values "
            "(id UInt64) ENGINE = Memory"
        )
        q(
            f"CREATE TABLE {database}.same_role_values "
            f"(id {database}.UserId) ENGINE = Memory"
        )
        q(
            f"INSERT INTO {database}.left_values VALUES "
            "(1, tuple(10), [1, 11]), (2, tuple(20), [2, 22])"
        )
        q(f"INSERT INTO {database}.right_values VALUES (2), (3)")
        q(f"INSERT INTO {database}.same_role_values VALUES (4)")

        # join_use_nulls must synthesize Nullable only on the missing side in
        # both orientations. The retained UDT schema endpoint must not make a
        # physical peer on the other side look like the same logical source.
        assert q(
            f"SELECT l.id, r.id FROM {database}.left_values AS l "
            f"LEFT JOIN {database}.right_values AS r ON l.id = r.id "
            "ORDER BY l.id FORMAT TSV SETTINGS join_use_nulls = 1"
        ) == "1\t\\N\n2\t2\n"
        assert q(
            f"SELECT l.id, r.id FROM {database}.left_values AS l "
            f"RIGHT JOIN {database}.right_values AS r ON l.id = r.id "
            "ORDER BY r.id FORMAT TSV SETTINGS join_use_nulls = 1"
        ) == "2\t2\n\\N\t3\n"

        # Query/Union paths cover unanimous, physical-only and conflicting
        # logical sources with an identical `UInt64` representation.
        assert q(
            f"SELECT id FROM (SELECT id FROM {database}.left_values "
            f"UNION ALL SELECT id FROM {database}.same_role_values) "
            "ORDER BY id FORMAT TSV"
        ) == "1\n2\n4\n"
        assert q(
            f"SELECT id FROM (SELECT id FROM {database}.left_values "
            f"UNION ALL SELECT id FROM {database}.right_values) "
            "ORDER BY id FORMAT TSV"
        ) == "1\n2\n2\n3\n"
        assert q(
            f"SELECT CAST(id AS {database}.UserId) FROM ("
            f"SELECT CAST(id AS {database}.UserId) AS id "
            f"FROM {database}.left_values "
            "UNION ALL "
            f"SELECT CAST(id AS {database}.AccountId) AS id "
            f"FROM {database}.right_values) ORDER BY id FORMAT TSV"
        ) == "1\n2\n2\n3\n"

        # One shared expression feeds equality, IN, tupleElement and
        # arrayElement consumers. This exercises analyzer clone/remap and CSE
        # without relying on formatted physical type equality for identity.
        assert q(
            f"WITH CAST(id AS {database}.UserId) AS shared "
            "SELECT shared, shared = CAST(2 AS "
            f"{database}.UserId), shared IN (CAST(1 AS {database}.UserId)), "
            "tupleElement(boxed, 'value'), arrayElement(ids, 1) "
            f"FROM {database}.left_values ORDER BY shared FORMAT TSV"
        ) == "1\t0\t1\t10\t1\n2\t1\t0\t20\t2\n"

        # A NULL-only arm, two distinct exact aliases, and an opaque arithmetic
        # function all remain executable but cannot manufacture provenance.
        assert q(
            f"SELECT if(id = 1, CAST(id AS {database}.UserId), NULL), "
            f"CAST(if(id = 1, CAST(id AS {database}.UserId), "
            f"CAST(id AS {database}.AccountId)) AS {database}.UserId), "
            f"CAST(toUInt64(id + 10) AS {database}.UserId) "
            f"FROM {database}.left_values ORDER BY id FORMAT TSV"
        ) == "1\t1\t11\n\\N\t2\t12\n"

        q(
            f"CREATE TABLE {database}.with_default ("
            f"id {database}.UserId DEFAULT toUInt64(7), "
            "payload UInt8) ENGINE = Memory"
        )
        q(f"INSERT INTO {database}.with_default (payload) VALUES (1)")
        assert q(f"SELECT id, payload FROM {database}.with_default FORMAT TSV") == "7\t1\n"

        # Recursive UNION is deliberately a provenance barrier; exact casts at
        # both sides must remain generation-safe through fixed-point cloning.
        assert q(
            "WITH RECURSIVE chain AS ("
            f"SELECT CAST(1 AS {database}.UserId) AS id "
            "UNION ALL "
            f"SELECT CAST(id + 1 AS {database}.UserId) FROM chain WHERE id < 3) "
            "SELECT groupArray(id) FROM chain SETTINGS max_threads = 1"
        ).strip() == "[1,2,3]"
    finally:
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_query_cache_revalidates_udt_binding_after_rename_revision_and_rebind(
    started_cluster,
):
    database = unique_database("cache")
    cache_query = f"SELECT sum(id) FROM {database}.events"
    try:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(f"CREATE TYPE {database}.AccountId AS UInt64")
        q(
            f"CREATE TABLE {database}.events "
            f"(id {database}.UserId) ENGINE = Memory"
        )
        q(f"INSERT INTO {database}.events VALUES (1), (2)")

        assert_cache_miss(cache_query, "3")
        assert_cache_hit(cache_query, "3")

        # The SQL and physical UInt64 result are unchanged. A hit is safe only
        # if the resident proof is rejected after the durable diagnostic rename.
        q(f"ALTER TYPE {database}.UserId RENAME TO PrincipalId")
        assert_cache_miss(cache_query, "3")
        assert_cache_hit(cache_query, "3")

        # Revision drift must invalidate independently of the retained name.
        q(f"ALTER TYPE {database}.PrincipalId COMMENT 'revision two'")
        assert_cache_miss(cache_query, "3")
        assert_cache_hit(cache_query, "3")

        # Rebinding the same storage UUID and physical column to a distinct UDT
        # changes its sidecar/identity while leaving the query text and bytes
        # unchanged. Neither old cache entry may win this race.
        q(
            f"ALTER TABLE {database}.events "
            f"MODIFY COLUMN id {database}.AccountId"
        )
        assert_cache_miss(cache_query, "3")
        assert_cache_hit(cache_query, "3")
    finally:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_query_cache_never_reuses_view_result_after_modify_query(started_cluster):
    database = unique_database("cache_view")
    cache_query = f"SELECT sum(value) FROM {database}.current_values"
    try:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TABLE {database}.before_values (value UInt64) ENGINE = Memory")
        q(f"CREATE TABLE {database}.after_values (value UInt64) ENGINE = Memory")
        q(f"INSERT INTO {database}.before_values VALUES (1), (2)")
        q(f"INSERT INTO {database}.after_values VALUES (10), (20)")
        q(
            f"CREATE VIEW {database}.current_values AS "
            f"SELECT value FROM {database}.before_values"
        )

        # A View dependency has no immutable definition-generation identity in
        # the pre-analyzer cache proof. It must therefore remain uncacheable.
        assert_cache_miss(cache_query, "3")
        assert_cache_miss(cache_query, "3")

        q(f"DROP VIEW {database}.current_values")
        q(
            f"CREATE VIEW {database}.current_values AS "
            f"SELECT value FROM {database}.after_values"
        )
        assert_cache_miss(cache_query, "30")
        assert_cache_miss(cache_query, "30")
    finally:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_query_cache_revalidates_complete_multi_storage_closure(started_cluster):
    database = unique_database("cache_closure")
    join_query = (
        f"SELECT sum(l.payload + r.payload) FROM {database}.left_events AS l "
        f"INNER JOIN {database}.right_events AS r USING (id)"
    )
    try:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.LeftId AS UInt64")
        q(f"CREATE TYPE {database}.RightId AS UInt64")
        q(
            f"CREATE TABLE {database}.left_events "
            f"(id {database}.LeftId, payload UInt64) ENGINE = Memory"
        )
        q(
            f"CREATE TABLE {database}.right_events "
            f"(id {database}.RightId, payload UInt64) ENGINE = Memory"
        )
        q(f"INSERT INTO {database}.left_events VALUES (1, 10), (2, 20)")
        q(f"INSERT INTO {database}.right_events VALUES (1, 1), (2, 2)")

        assert_cache_miss(join_query, "33")
        assert_cache_hit(join_query, "33")

        # Either side is an independently rooted cache dependency.  Changing
        # only one definition must invalidate the same cached SQL and bytes.
        q(f"ALTER TYPE {database}.LeftId COMMENT 'left revision two'")
        assert_cache_miss(join_query, "33")
        assert_cache_hit(join_query, "33")

        q(f"ALTER TYPE {database}.RightId COMMENT 'right revision two'")
        assert_cache_miss(join_query, "33")
        assert_cache_hit(join_query, "33")

        # Rebinding just one storage to a fresh, physically compatible type is
        # a distinct invalidation from definition revision drift.
        q(f"CREATE TYPE {database}.ReplacementId AS UInt64")
        q(
            f"ALTER TABLE {database}.right_events "
            f"MODIFY COLUMN id {database}.ReplacementId"
        )
        assert_cache_miss(join_query, "33")
        assert_cache_hit(join_query, "33")
    finally:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"DROP DATABASE IF EXISTS {database} SYNC")


def test_contextual_udt_candidates_never_take_pre_analyzer_cache_hit(
    started_cluster,
):
    database = unique_database("cache_context")
    try:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"CREATE DATABASE {database} ENGINE = Atomic")
        q(f"CREATE TYPE {database}.UserId AS UInt64")
        q(
            f"CREATE TABLE {database}.events "
            f"(id {database}.UserId, ids Array({database}.UserId)) ENGINE = Memory"
        )
        q(
            f"INSERT INTO {database}.events VALUES "
            "(1, [1, 11]), (2, [2, 22]), (3, [])"
        )

        safe_query = f"SELECT sum(id) FROM {database}.events"
        assert_cache_miss(safe_query, "6")
        assert_cache_hit(safe_query, "6")

        # These closed syntax classes can acquire a contextual logical role
        # only during QueryAnalyzer.  A physical-only entry must therefore not
        # bypass analysis, even when the previous result has identical bytes.
        candidates = [
            (f"SELECT count() FROM {database}.events WHERE id = 1", "1"),
            (f"SELECT count() FROM {database}.events WHERE id IN (1, 3)", "2"),
            (f"SELECT count() FROM {database}.events WHERE has(ids, 2)", "1"),
            (
                f"SELECT count() FROM {database}.events "
                "WHERE hasAny(ids, [11, 99])",
                "1",
            ),
            (
                f"SELECT count() FROM {database}.events WHERE id GLOBAL IN "
                f"(SELECT id FROM {database}.events WHERE id != 2)",
                "2",
            ),
        ]
        for candidate_query, expected in candidates:
            assert_cache_miss(candidate_query, expected)
            assert_cache_hit(candidate_query, expected)

        # Each candidate now has a resident entry.  Definition revision drift
        # must reject it before analysis; the newly published complete proof is
        # reusable only after that full analysis has run once.
        q(f"ALTER TYPE {database}.UserId COMMENT 'context revision two'")
        for candidate_query, expected in candidates:
            assert_cache_miss(candidate_query, expected)
            assert_cache_hit(candidate_query, expected)

        # The same revision invalidates the neighboring non-contextual entry
        # once; publishing all contextual proofs must not keep it disabled.
        assert_cache_miss(safe_query, "6")
        assert_cache_hit(safe_query, "6")
    finally:
        q("SYSTEM CLEAR QUERY CACHE")
        q(f"DROP DATABASE IF EXISTS {database} SYNC")
