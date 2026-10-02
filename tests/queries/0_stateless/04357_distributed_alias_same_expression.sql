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

-- The collapse may happen inside a subquery that feeds an outer query. The subquery's distributed read is
-- renumbered independently from the initiator's query tree, so reconciling the collapsed shard header by column
-- name has to account for the differing `__tableN` table aliases.
SELECT count() FROM (SELECT a1, a2 FROM dist_same_expr GROUP BY a1, a2);
SELECT count() AS groups, sum(c) AS total FROM (SELECT a1, a2, count() AS c FROM dist_same_expr GROUP BY a1, a2);
SELECT a1 FROM (SELECT a1, a2 FROM dist_same_expr GROUP BY a1, a2) WHERE a1 = '7';
SELECT count() FROM (SELECT a1, a2 FROM (SELECT a1, a2 FROM dist_same_expr GROUP BY a1, a2));
-- A non-collapsed ALIAS column inside the subquery must keep flowing alongside the collapsed pair.
SELECT a1, a2, b FROM (SELECT a1, a2, b FROM dist_same_expr GROUP BY a1, a2, b) ORDER BY a1;

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

-- Adversarial: user text that looks like a planner table qualifier `__tableN.` must not perturb the collapse
-- reconstruction. The reconstruction matches shard columns to expected columns after erasing the numbering of only
-- the GENUINE `__tableN.` qualifiers, those whose tail is a real column name collected from the query tree. Any
-- look-alike text (a column literally named `__table9`, the string constants `'__table9'` / `'__table1.'`, an
-- over-long `__table999...` run, a backquoted column named `__table1.k`, or a lambda argument named `__table1.y`) is
-- not a genuine qualifier, so it is left untouched and two distinct such values never collapse onto one another. Each case rides
-- along a duplicate-ALIAS collapse nested in a subquery, the case that triggers the renumbering between the shard and
-- initiator trees.
DROP TABLE IF EXISTS loc_adv;
DROP TABLE IF EXISTS dist_adv;

CREATE TABLE loc_adv
(
    dt DateTime,
    x UInt8,
    `__table9` UInt8,
    `__table1.k` UInt8,
    a1 String ALIAS toString(x),
    a2 String ALIAS toString(x),
    -- ALIAS columns whose expression is a lambda. The lambda argument name `__table1.y` is user text emitted into the
    -- action name unquoted, so it is indistinguishable in shape from a real table qualifier (`__tableN.<tail>`), yet its
    -- tail `y` is not a real column name, so the qualifier blanking must leave it untouched.
    lamy0 Array(String) ALIAS arrayMap(`__table1.y` -> toString(x), [0]),
    lamy1 Array(String) ALIAS arrayMap(`__table1.y` -> toString(x), [0])
)
ENGINE = MergeTree ORDER BY dt;
CREATE TABLE dist_adv AS loc_adv
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), loc_adv, rand());
INSERT INTO loc_adv (dt, x, `__table9`, `__table1.k`) VALUES ('2024-01-01 00:00:00', 7, 5, 3);

-- A string constant `'__table9'` and an arithmetic over a column named `__table9` next to the collapsed (a1, a2)
-- pair. Neither `__table9` token is a real qualifier (no trailing dot), so neither side's name is altered by the
-- qualifier blanking and the collapsed pair still reconciles.
SELECT a1, a2, w, n FROM
(
    SELECT a1, a2, concat('__table9', a1) AS w, `__table9` + 1 AS n
    FROM dist_adv GROUP BY a1, a2, `__table9`
) ORDER BY a1;

-- A string constant with an over-long digit run. The digits are never parsed into an integer, so an over-long run
-- cannot overflow, and the collapsed pair still reconciles.
SELECT a1, a2, c FROM
(
    SELECT a1, a2, concat('__table99999999999999999999999999999.col', a1) AS c
    FROM dist_adv GROUP BY a1, a2
) ORDER BY a1;

-- A string constant `'__table1.'` that mimics a complete qualifier (digits + trailing dot). It is a quoted span, not a
-- genuine qualifier, so its number is left intact and the collapsed pair reconciles instead of failing with
-- NUMBER_OF_COLUMNS_DOESNT_MATCH.
SELECT a1, a2, v FROM
(
    SELECT a1, a2, concat('__table1.', a1) AS v
    FROM dist_adv GROUP BY a1, a2
) ORDER BY a1;

-- Two distinct string constants `'__table1.'` and `'__table2.'` that each mimic a complete qualifier, computed over a
-- shard-side expression (`toString(x)`) so both land in the shard projection next to the collapsed (a1, a2) pair.
-- Their digits must NOT be erased: they are user text, not genuine qualifiers (a genuine qualifier's tail is a real
-- column name), so the two columns stay distinct and the collapse reconciles. Erasing them would make both canonicalize
-- to the same name, drop one as a duplicate, and fail with NUMBER_OF_COLUMNS_DOESNT_MATCH.
SELECT a1, a2, v1, v2 FROM
(
    SELECT a1, a2, concat('__table1.', toString(x)) AS v1, concat('__table2.', toString(x)) AS v2
    FROM dist_adv GROUP BY a1, a2, v1, v2
) ORDER BY a1;

-- A backquoted column identifier `__table1.k` that looks like a qualifier with a trailing dot. It is the rendered
-- name of a real column (`backQuoteIfNeed` quotes the dot), so it is matched as a whole tail and not mistaken for a
-- `__table1.` qualifier followed by a column `k`.
SELECT a1, a2, m FROM
(
    SELECT a1, a2, `__table1.k` AS m
    FROM dist_adv GROUP BY a1, a2, `__table1.k`
) ORDER BY a1;

-- A lambda argument name leaks into the action name unquoted. `__table1.y` looks like a qualifier (digits + dot +
-- tail), but its tail `y` is not a real column name, so it is not treated as a genuine qualifier and its numbering is
-- left intact. The collapsed (lamy0, lamy1) pair reconciles instead of failing with NUMBER_OF_COLUMNS_DOESNT_MATCH.
SELECT lamy0, lamy1 FROM
(
    SELECT lamy0, lamy1 FROM dist_adv GROUP BY lamy0, lamy1
) ORDER BY lamy0;

DROP TABLE dist_adv;
DROP TABLE loc_adv;
