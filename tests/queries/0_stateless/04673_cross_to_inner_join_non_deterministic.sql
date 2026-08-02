-- The old (non-analyzer) rewriter duplicates the predicate into ON and keeps the whole WHERE, so it
-- answers correctly and every live row below would be vacuous with enable_analyzer = 0.
-- A session SET also survives `compatibility` randomization, which can flip the analyzer off.
SET enable_analyzer = 1;
-- Keep the sibling optimizer pass `tryMergeFilterIntoJoinCondition` out, so this test measures only
-- CrossToInnerJoinPass.
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS r SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS m SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS w SETTINGS ignore_drop_queries_probability = 0;
DROP DICTIONARY IF EXISTS dict SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS dsrc SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE l (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE m (a UInt64) ENGINE = Log;
-- A separate table for the rows that need a Dynamic or Variant column: Log supports neither.
CREATE TABLE w (a UInt64, rate Float64, d Dynamic, v Variant(Float64, String)) ENGINE = MergeTree ORDER BY a;

INSERT INTO l SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO r SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO m SELECT number % 4 FROM numbers(40);
INSERT INTO w SELECT number % 16, 0.1,
    (number / 100.)::Float64::Dynamic, (number / 100.)::Float64::Variant(Float64, String)
FROM numbers(200);

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

-- randConstant() is stable within one node's query but is drawn again wherever its function base is
-- built, so it belongs to the same class as queryID() below. The argument is what keeps it a live node:
-- with no argument it is constant-folded before this pass runs.
SELECT '-- randConstant() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE randConstant(l.a) % 16 = r.a
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

-- getMacro() is the server constant that is documented to differ per node by design, so it is the
-- clearest member of the class.
SELECT '-- getMacro() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE concat(toString(l2.a), getMacro('shard'))
        = concat(toString(r.a), getMacro('shard'))
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

-- `getServerPort` now reports `isServerConstant` on both its function base and its overload resolver.
-- It is folded on a single-node query like the rows above, so it needs remote() too.
SELECT '-- getServerPort() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + getServerPort('tcp_port') = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- transactionID() is the same class again: it reads the executing node's current transaction in its
-- constructor. Its two snapshot counters read a process-wide counter in theirs, so all three of the
-- classes in FunctionsTransactionCounters.cpp need the name. The stateless test config enables
-- transactions, so all three are reachable.
SELECT '-- transactionID() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + toUInt64(transactionID().1) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- transactionLatestSnapshot() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + transactionLatestSnapshot() = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- transactionOldestSnapshot() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM remote('127.0.0.1', currentDatabase(), l) AS l2, r
    WHERE l2.a + transactionOldestSnapshot() = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- The filesystem* family reads the disks of the node that runs it, so it belongs to the same class
-- again. Unlike the server constants above it is not folded away on a single node, because a
-- non-constant argument sends it down a folding path whose default result is "not a constant". The
-- argument must therefore stay non-constant: filesystemCapacity('default') IS folded and such a row
-- would read the same on master.
SELECT '-- filesystemCapacity() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + filesystemCapacity(materialize('default')) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- filesystemAvailable() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + filesystemAvailable(materialize('default')) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- filesystemUnreserved() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + filesystemUnreserved(materialize('default')) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- getClientHTTPHeader() reads the headers of the request being served, which its own documentation
-- says are non-empty only on the initiator of a distributed query. The argument must be non-constant
-- for the same reason as the filesystem* rows above.
SELECT '-- getClientHTTPHeader() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), getClientHTTPHeader(materialize('X-Test')))
        = concat(toString(r.a), getClientHTTPHeader(materialize('X-Test')))
    SETTINGS cross_to_inner_join_rewrite = 1, allow_get_client_http_header = 1
) WHERE explain ILIKE '%kind: INNER%';

-- The three address symbolizers resolve an address against the object files and the address space
-- layout of the node that runs them, so they are node-local too, and like showCertificate() they report
-- no determinism predicate at all. The argument must be non-constant for the same reason as the
-- filesystem* rows above. These rows only read the plan shape: EXPLAIN QUERY TREE never executes the
-- predicate, so no symbolization is attempted and no platform tag is needed.
SELECT '-- addressToSymbol() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + length(addressToSymbol(l.a)) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1, allow_introspection_functions = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- addressToLine() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + length(addressToLine(l.a)) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1, allow_introspection_functions = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- addressToLineWithInlines() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + length(addressToLineWithInlines(l.a)) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1, allow_introspection_functions = 1
) WHERE explain ILIKE '%kind: INNER%';

-- financialNetPresentValueExtended() also reports isDeterministic() = false, but it is stable within
-- one node's query and every node computes the same value, so it must stay eligible. Over a Dynamic or
-- a Variant argument it is additionally wrapped in an adaptor that declines constant folding, which is
-- the shape a guard written as "not deterministic and not foldable" would wrongly reject.
SELECT '-- financialNetPresentValueExtended() over a Dynamic argument is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM w, r
    WHERE w.a + toUInt64(financialNetPresentValueExtended(w.d, [100, 200]::Array(Float64),
        ['2020-01-01', '2021-01-01']::Array(Date))) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- financialNetPresentValueExtended() over a Variant argument is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM w, r
    WHERE w.a + toUInt64(financialNetPresentValueExtended(w.v, [100, 200]::Array(Float64),
        ['2020-01-01', '2021-01-01']::Array(Date))) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- financialNetPresentValueExtended() over an ordinary column is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM w, r
    WHERE w.a + toUInt64(financialNetPresentValueExtended(w.rate, [100, 200]::Array(Float64),
        ['2020-01-01', '2021-01-01']::Array(Date))) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP DICTIONARY dict SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE dsrc SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE r SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE m SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE w SETTINGS ignore_drop_queries_probability = 0;
