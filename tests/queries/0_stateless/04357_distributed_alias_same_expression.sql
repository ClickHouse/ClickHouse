-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106403
-- Two (or more) ALIAS columns expanding to the same expression are deduplicated into a single
-- column on the shard, while the initiator keeps them distinct. This must reconcile without a
-- NUMBER_OF_COLUMNS_DOESNT_MATCH exception on a Distributed table / with parallel replicas.

DROP TABLE IF EXISTS local_same_expr;
DROP TABLE IF EXISTS dist_same_expr;

CREATE TABLE local_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x),
    `b` UInt8 ALIAS x + 1
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_same_expr
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x),
    `b` UInt8 ALIAS x + 1
)
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_same_expr, rand());

INSERT INTO local_same_expr (dt, x) VALUES ('2024-01-01 00:00:00', 7);

SET enable_analyzer = 1;

-- Basic case: two ALIAS columns with same expression + ORDER BY on a non-projected column
SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC;

SELECT a1, a2, dt FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Renamed ALIAS projections: user applies AS rename
SELECT a1 AS first, a2 AS second FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Repeated ALIAS projection: same ALIAS column selected twice
SELECT a1, a2, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Mixed: ALIAS column and the same expression written directly.
SELECT a1, toString(x) FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Interleaved repetitions.
SELECT a1, a2, a1 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;
SELECT a1, a1, a2, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- Pure user-written duplication, no ALIAS columns involved.
SELECT toString(x), toString(x) FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- A third ALIAS column with a different expression must not be collapsed with the others.
SELECT a1, a2, b FROM dist_same_expr ORDER BY dt DESC LIMIT 1;

-- GROUP BY on duplicate ALIAS columns.
SELECT a1, a2 FROM dist_same_expr GROUP BY a1, a2 ORDER BY a1;
SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2 ORDER BY a1;

-- HAVING referencing a duplicate ALIAS column.
SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2 HAVING a1 != '' ORDER BY a1;

-- Aggregate over a duplicate ALIAS expression.
SELECT a1, toString(x), sum(x) AS s FROM dist_same_expr GROUP BY a1, toString(x) ORDER BY a1;

-- DISTINCT over duplicate ALIAS columns.
SELECT DISTINCT a1, a2 FROM dist_same_expr ORDER BY a1;

-- Same scenarios with parallel replicas reading from the local MergeTree table.
SET allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT a1, a2 FROM local_same_expr ORDER BY dt DESC LIMIT 1;
SELECT a1, a2, count() AS c FROM local_same_expr GROUP BY a1, a2 ORDER BY a1;

-- Single hop with serialize_query_plan: the plan-serialization transport must reconcile the
-- collapsed shard header too.
SELECT a1, a2 FROM dist_same_expr ORDER BY dt DESC LIMIT 1 SETTINGS serialize_query_plan = 1;
SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2 ORDER BY a1 SETTINGS serialize_query_plan = 1;

SET allow_experimental_parallel_reading_from_replicas = 0;

DROP TABLE dist_same_expr;
DROP TABLE local_same_expr;

-- Multi-hop: Distributed over Distributed. Duplicate ALIAS columns expanding to the same
-- expression must survive two transport hops without a column-count mismatch, including over
-- the plan-serialization transport.
DROP TABLE IF EXISTS dod_local;
DROP TABLE IF EXISTS dod_inner;
DROP TABLE IF EXISTS dod_outer;

CREATE TABLE dod_local (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dod_local VALUES (1), (2), (10);

CREATE TABLE dod_inner
(
    x UInt64,
    a UInt64 ALIAS 2,
    b UInt64 ALIAS 2,
    inner_c UInt64 ALIAS x + 1,
    inner_d UInt64 ALIAS x + 1
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), dod_local);

CREATE TABLE dod_outer
(
    x UInt64,
    inner_c UInt64,
    a UInt64 ALIAS 1,
    b UInt64 ALIAS 1,
    c UInt64 ALIAS inner_c,
    d UInt64 ALIAS inner_c,
    inner_d UInt64
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), dod_inner);

SELECT 'multi_hop prefer_localhost_replica=0';
SELECT x, a, b, c, d, inner_c, inner_d FROM dod_outer ORDER BY x SETTINGS prefer_localhost_replica = 0;
SELECT 'multi_hop prefer_localhost_replica=1';
SELECT x, a, b, c, d, inner_c, inner_d FROM dod_outer ORDER BY x SETTINGS prefer_localhost_replica = 1;
SELECT 'multi_hop serialize_query_plan=1';
SELECT x, a, b, c, d, inner_c, inner_d FROM dod_outer ORDER BY x SETTINGS serialize_query_plan = 1;

DROP TABLE dod_outer;
DROP TABLE dod_inner;
DROP TABLE dod_local;

-- Second hop: remote() over a Distributed table with parallel replicas. Duplicate ALIAS columns
-- expanding to the same expression must survive the remote -> Distributed -> parallel-replicas
-- fan-out without a column-count mismatch.
DROP TABLE IF EXISTS ph_local;
DROP TABLE IF EXISTS ph_dist;

CREATE TABLE ph_local
(
    dt DateTime64(3),
    base String,
    alias_base_0 String ALIAS base,
    alias_base_1 String ALIAS base
)
ENGINE = MergeTree ORDER BY dt;
INSERT INTO ph_local VALUES ('1999-03-29T01:15:33', 'x'), ('1999-03-29T01:15:34', 'y');

CREATE TABLE ph_dist AS ph_local
ENGINE = Distributed('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), ph_local);

SELECT 'second_hop single replica';
SELECT dt, alias_base_0, alias_base_1 FROM remote('127.0.0.2', currentDatabase(), ph_dist) ORDER BY dt LIMIT 1
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SELECT 'second_hop parallel replicas';
SELECT dt, alias_base_0, alias_base_1 FROM remote('127.0.0.2', currentDatabase(), ph_dist) ORDER BY dt LIMIT 1
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE ph_dist;
DROP TABLE ph_local;

-- Duplicate ALIAS columns whose expression is an over-threshold constant. The shard rewrites such
-- constants into `__getScalar('<hash>')` (see ReplaceLongConstWithScalarVisitor, controlled by
-- `optimize_const_name_size`, default 256), so the collapsed shard column is named after the scalar,
-- not after the literal. The reconstruction must account for that rewrite.
DROP TABLE IF EXISTS loc_longlit;
DROP TABLE IF EXISTS dist_longlit;

CREATE TABLE loc_longlit
(
    dt DateTime,
    x UInt8,
    a1 String ALIAS repeat('y', 300),
    a2 String ALIAS repeat('y', 300)
)
ENGINE = MergeTree ORDER BY dt;
CREATE TABLE dist_longlit AS loc_longlit
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), loc_longlit, rand());
INSERT INTO loc_longlit (dt, x) VALUES ('2024-01-01 00:00:00', 7);

-- Pin the threshold so the 300-byte literal is guaranteed to be scalarized regardless of the
-- default value of `optimize_const_name_size` (which may change in the future).
SET optimize_const_name_size = 64;

SELECT length(a1), length(a2) FROM dist_longlit ORDER BY dt DESC LIMIT 1;
SELECT a1 = a2 FROM dist_longlit ORDER BY dt DESC LIMIT 1;
SELECT length(a1) AS l, length(a2) AS m, count() AS c FROM dist_longlit GROUP BY a1, a2 ORDER BY l;

DROP TABLE dist_longlit;
DROP TABLE loc_longlit;
