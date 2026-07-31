-- The old (non-analyzer) rewriter duplicates the predicate into ON and keeps the whole WHERE, so it
-- answers correctly and every live row below would be vacuous with enable_analyzer = 0.
-- A session SET also survives `compatibility` randomization, which can flip the analyzer off.
SET enable_analyzer = 1;
-- Keep the sibling optimizer pass `tryMergeFilterIntoJoinCondition` out, so this test measures only
-- CrossToInnerJoinPass.
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS m;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dsrc;

CREATE TABLE l (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE m (a UInt64) ENGINE = Log;

INSERT INTO l SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO r SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO m SELECT number % 4 FROM numbers(40);

CREATE TABLE dsrc (k UInt64, v UInt64) ENGINE = Log;
INSERT INTO dsrc SELECT number, number % 16 FROM numbers(64);
CREATE DICTIONARY dict (k UInt64, v UInt64)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dsrc')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- The second conjunct is single-side, so it is never extracted into the join and pins the key to 3.
-- Every surviving r.a must therefore be 3. Row counts are not asserted: they are random here.

SELECT '-- comma join';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- explicit CROSS JOIN';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l CROSS JOIN r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- non-deterministic on the right side of the equality';
SELECT uniqExact(l.a) = 1 AND min(l.a) = 3 AND max(l.a) = 3 AND count() > 0
FROM l, r WHERE l.a = rand(r.a) % 16 AND 3 = rand(r.a) % 16
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- three tables, one non-deterministic edge';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r, m WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3 AND m.a = l.a % 4
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- generateUUIDv4';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r
WHERE toUInt64(reinterpretAsUInt128(generateUUIDv4(l.a))) % 16 = r.a
  AND toUInt64(reinterpretAsUInt128(generateUUIDv4(l.a))) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- forced rewrite, non-deterministic and deterministic edge';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE rand(l.a) % 16 = r.a AND rand(l.a) % 16 = 3 AND l.b = r.b
SETTINGS cross_to_inner_join_rewrite = 2;

-- The rewrite is not attempted for a non-deterministic condition, so with no other equi condition a
-- forced rewrite reports the pre-existing error instead of silently returning wrong results.
SELECT '-- forced rewrite, only a non-deterministic edge';
SELECT count() FROM l, r WHERE rand(l.a) % 16 = r.a
SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }

SELECT '-- forced rewrite, no equi condition at all';
SELECT count() FROM l, r WHERE l.a > r.a
SETTINGS cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }

SELECT '-- deterministic predicate is unaffected';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE (l.a * 1) % 16 = r.a AND (l.a * 1) % 16 = 3
SETTINGS cross_to_inner_join_rewrite = 1;

SELECT '-- deterministic predicate is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- dictGet() reports isDeterministic() = false, but every node reads the same dictionary by name, so it
-- stays eligible. This is the row that catches a guard written against isDeterministic() instead of
-- isDeterministicInScopeOfQuery(): now() and currentUser() are constant-folded before this pass runs,
-- so they cannot serve that purpose.
SELECT '-- dictGet() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE dictGet(currentDatabase() || '.dict', 'v', l.a) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- dictGet() answers correctly';
SELECT uniqExact(r.a) = 1 AND min(r.a) = 3 AND max(r.a) = 3 AND count() > 0
FROM l, r WHERE dictGet(currentDatabase() || '.dict', 'v', l.a) = r.a
  AND dictGet(currentDatabase() || '.dict', 'v', l.a) = 3
SETTINGS cross_to_inner_join_rewrite = 1;

-- queryID() and FQDN() are read once per executing node instead of once per query, so the two sides of
-- a key can be built by different nodes and compare values that never match.
SELECT '-- queryID() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), queryID()) = concat(toString(r.a), queryID())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- FQDN() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), FQDN()) = concat(toString(r.a), FQDN())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- timeSeriesTagsToGroup() is query-deterministic and is not constant-folded, so only the isStateful()
-- clause can refuse it.
SELECT '-- a stateful function is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE timeSeriesTagsToGroup([], 'k', toString(l.a)) % 16 = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- now() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + toUInt64(now()) = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- currentUser() is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), currentUser()) = concat(toString(r.a), currentUser())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- rand() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE rand(l.a) % 16 = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- nowInBlock() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + toUInt64(nowInBlock()) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- Server constants (hostName, shardNum, uptime, ... reporting isServerConstant()) are read per
-- executing node, so they belong to the same class as queryID() above. On a single-node query they are
-- constant-folded away, which is why each row below joins a remote() table expression: that sets the
-- query's is_distributed flag, isSuitableForConstantFolding() becomes false, and the function reaches
-- this pass as a live node. Without remote() these rows would pass on master too.
SELECT '-- hostName() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE concat(toString(l2.a), hostName()) = concat(toString(r.a), hostName())
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- shardNum() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + shardNum() = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- uptime() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + uptime() = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- A remote() join with a deterministic predicate must still be rewritten, so the three rows above fail
-- for the server constant rather than merely for being distributed.
SELECT '-- a deterministic remote() predicate is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- getServerPort() is in the same class but does not report isServerConstant(), so only the name list
-- can refuse it. It is folded on a single-node query like the rows above, so it needs remote() too.
SELECT '-- getServerPort() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + getServerPort('tcp_port') = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- transactionID() is the same class again: it reads the executing node's current transaction in its
-- constructor. Its two sibling counters throw without allow_experimental_transactions, but this one
-- does not, so it is reachable and needs the name.
SELECT '-- transactionID() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + toUInt64(transactionID().1) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP DICTIONARY dict;
DROP TABLE dsrc;
DROP TABLE l;
DROP TABLE r;
DROP TABLE m;
