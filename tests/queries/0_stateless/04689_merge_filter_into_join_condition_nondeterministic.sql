-- A non-deterministic cross-side equality must not become a JOIN key: the conjunct above the join is
-- replaced by a constant, so the key draws its own value and the surviving rows do not satisfy it.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_filter_into_join_condition = 1;

CREATE TABLE l (k UInt32, a UInt32) ENGINE = Memory;
CREATE TABLE r (k UInt32, b UInt8) ENGINE = Memory;
INSERT INTO l SELECT number % 4, number FROM numbers(400);
INSERT INTO r SELECT number, number FROM numbers(4);

-- Every row returned must satisfy `x = b`, which is the WHERE. Before the fix about 75 of 100 do not.
SELECT count() FROM (
    SELECT toUInt8(rand(l.a) % 4) AS x, r.b AS b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(rand(l.a) % 4) = r.b)
WHERE x != b;

-- A deterministic expression in the same shape is still merged, so the guard is not refusing everything.
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- `runningConcurrency` is stateful while reporting itself deterministic. Its value also depends on
-- the physical join, so assert the plan, which is what the guard decides.
CREATE TABLE lc (k UInt32, s DateTime, e DateTime) ENGINE = Memory;
CREATE TABLE rc (k UInt32, b UInt8) ENGINE = Memory;
INSERT INTO lc SELECT 1, toDateTime(1700000000 + intDiv(number, 4)), toDateTime(1700000000 + intDiv(number, 4) + 3000) FROM numbers(2000);
INSERT INTO rc SELECT 1, number % 16 FROM numbers(16);
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT rc.b FROM lc JOIN rc ON lc.k = rc.k
    WHERE toUInt8(runningConcurrency(lc.s, lc.e) % 16) = rc.b
    SETTINGS explain_query_plan_default = 'legacy');

-- `byteSize` reads the physical representation: on the sparse right column it reports 19, while the
-- value the JOIN produces is the dense 11, which is what the predicate means.
CREATE TABLE lb (k UInt32, x UInt64) ENGINE = Memory;
CREATE TABLE rb (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO lb SELECT number % 8, 11 FROM numbers(80);
INSERT INTO rb SELECT number % 8, if(number % 63 = 0, 'abc', '') FROM numbers(1024);
SELECT count() > 0 FROM lb JOIN rb ON lb.k = rb.k WHERE lb.x = byteSize(rb.s);

-- `joinGet` declares its non-determinism on the overload resolver, not on the function the plan holds.
CREATE TABLE jt (k UInt32, v UInt8) ENGINE = Join(ANY, LEFT, k);
INSERT INTO jt SELECT number, number % 4 FROM numbers(100);
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(joinGet(currentDatabase() || '.jt', 'v', toUInt32(l.a)) % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- The name the plan holds follows the storage's `join_use_nulls`, so this is a second spelling to refuse.
CREATE TABLE jtn (k UInt32, v UInt8) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO jtn SELECT number, number % 4 FROM numbers(100);
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(assumeNotNull(joinGet(currentDatabase() || '.jtn', 'v', toUInt32(l.a))) % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- The hierarchy lookups also carry their non-determinism only on the resolver, and they read the
-- dictionary at execution time rather than caching it.
CREATE TABLE hsrc (id UInt64, parent_id UInt64) ENGINE = Memory;
INSERT INTO hsrc VALUES (1, 0), (2, 1), (3, 1), (4, 2);
CREATE DICTIONARY hdict (id UInt64, parent_id UInt64 HIERARCHICAL)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'hsrc')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1);
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(length(dictGetDescendants(currentDatabase() || '.hdict', toUInt64(l.a % 4 + 1))) % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(length(dictGetChildren(currentDatabase() || '.hdict', toUInt64(l.a % 4 + 1))) % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- `arrayJoin` is a node type rather than a function, so it needs its own refusal.
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(arrayJoin([l.a, l.a + 1]) % 4) = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- A non-deterministic call inside a lambda body, which is not a child of its wrapper.
SELECT count() FROM (
    SELECT arrayMap(z -> toUInt8(rand(z) % 4), [l.a])[1] AS x, r.b AS b FROM l JOIN r ON l.k = r.k
    WHERE arrayMap(z -> toUInt8(rand(z) % 4), [l.a])[1] = r.b)
WHERE x != b;

-- The same lambda over a deterministic body must still be merged.
SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT r.b FROM l JOIN r ON l.k = r.k
    WHERE arrayMap(z -> toUInt8(z % 4), [l.a])[1] = r.b
    SETTINGS explain_query_plan_default = 'legacy');

-- The right operand is guarded too, not only the left.
SELECT count() FROM (
    SELECT toUInt8(rand(r.b) % 4) AS x, toUInt8(l.a % 4) AS la FROM l JOIN r ON l.k = r.k
    WHERE toUInt8(l.a % 4) = toUInt8(rand(r.b) % 4))
WHERE x != la;

DROP TABLE jt;
DROP TABLE jtn;
DROP DICTIONARY hdict;
DROP TABLE hsrc;
DROP TABLE lb;
DROP TABLE rb;
DROP TABLE lc;
DROP TABLE rc;
DROP TABLE l;
DROP TABLE r;
