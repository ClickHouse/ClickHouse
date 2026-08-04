-- Tags: shard, no-old-analyzer
-- The `Distributed` rewrite wraps an `ALIAS` column expansion into `__actionName` only
-- when the expansion may collapse a column of the block sent over the network: when a
-- whole expression of a query clause coincides, after inlining, with another whole
-- clause expression that the initiator distinguishes. Subexpressions of larger
-- expressions and filter trees are not collision candidates, so no wrapper (it is
-- opaque to the planner and would pessimize shard-side optimization) must appear for
-- them. The rewritten query the initiator sends to the shard is read back from
-- system.query_log (scoped by log_comment and the current database name inside the
-- query text), which pins the presence and the absence of the `__actionName` wrapper.
-- The rewrite lives in the analyzer's distributed planning (`buildQueryTreeDistributed`),
-- so the test is restricted to the analyzer.

DROP TABLE IF EXISTS shard_04700;
DROP TABLE IF EXISTS dist_04700;

CREATE TABLE shard_04700
(
    a String,
    b Float64,
    c Float64,
    lit UInt8 ALIAS 1,
    ref Float64 ALIAS b,
    d Float64 ALIAS b + c
)
ENGINE = MergeTree() ORDER BY a;

INSERT INTO shard_04700 VALUES ('x', 1, 2);

CREATE TABLE dist_04700 AS shard_04700
ENGINE = Distributed(test_shard_localhost, currentDatabase(), shard_04700);

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

-- An ALIAS column of a constant next to an equal sibling literal inside one expression:
-- only one output column exists, nothing can collapse, no wrapper.
SELECT lit + 1 FROM dist_04700 SETTINGS log_comment = '04700_sibling_literal';

-- An ALIAS column of a plain column reference, with the referenced column used only in
-- the filter: filters do not contribute transmitted columns, no wrapper.
SELECT ref FROM dist_04700 WHERE b = 1 SETTINGS log_comment = '04700_filter_column';

-- The ALIAS expansion is a subexpression of one projection entry, and the inlined form
-- of that whole entry coincides with the other entry: the initiator distinguishes
-- sum(d) and sum(b + c), the shard must not collapse them, the wrapper is required.
SELECT sum(d), sum(b + c) FROM dist_04700 SETTINGS log_comment = '04700_nested_collision';

-- An ALIAS column of a constant selected next to an equal literal: two whole projection
-- entries with one inlined form, the wrapper is required.
SELECT 1, lit FROM dist_04700 SETTINGS log_comment = '04700_constant_collision';

-- An ALIAS column of a plain column reference selected next to that column: the wrapper
-- is required (issue #108291).
SELECT ref, b FROM dist_04700 SETTINGS log_comment = '04700_physical_collision';

SYSTEM FLUSH LOGS query_log;

-- For each query above: the query sent to the shard must be logged, and must contain
-- the `__actionName` wrapper exactly when a collapse had to be prevented.
SELECT
    log_comment,
    count() > 0 AS shard_query_logged,
    countIf(query LIKE '%__actionName%') > 0 AS wrapped
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
    AND log_comment LIKE '04700\_%'
    AND query LIKE concat('%', currentDatabase(), '%')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE dist_04700;
DROP TABLE shard_04700;
