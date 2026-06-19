-- Tags: no-parallel-replicas

-- When a residual filter is pushed to a table scan but cannot be used by the primary index,
-- the exact row count is lost, but the number of rows scanned (`selected_rows`) is still a
-- valid upper bound on the rows fed into the join. The build-side choice ('auto') uses this
-- bound: if one input's upper bound is below the other input's exact estimate, the bounded
-- input is provably smaller and must become the build side.
--
-- Here `small` (1000 rows) carries a non-key residual filter, so only its upper bound (1000)
-- is known, while `big` (1000000 rows) has an exact estimate. Even though `small` is written
-- first (probe by default), it must be swapped to the build side because 1000 < 1000000.

SET enable_analyzer = 1;
SET use_statistics = 0;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO small SELECT number, number FROM numbers(1000);

CREATE TABLE big (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO big SELECT number FROM numbers(1000000);

-- `small.v < 1000000` keeps every row of `small` but is not usable by the index on `k`.
SELECT * FROM small JOIN big ON small.k = big.k WHERE small.v < 1000000
SETTINGS log_comment = '04337_join_choose_build_table_upper_bound' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- `small` (upper bound 1000) must be the build side; `big` (exact 1000000) the probe side.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 900 AND 1100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 900000 AND 1100000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 1000, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04337_join_choose_build_table_upper_bound'
ORDER BY event_time DESC
LIMIT 1;

-- Soundness: `small.k != -1` is a predicate on the primary key, but one the index cannot use
-- to prune -- it is true for every row, so no granule is excluded and the row count stays an
-- upper bound rather than an exact estimate (an index pruning a column only yields an exact
-- count when it actually drops granules, e.g. `k < 500`). The predicate is also inferred onto
-- the other side via transitive equi-join predicates, so BOTH inputs are upper-bound-only.
-- Two upper bounds cannot prove which input is smaller (`big`'s true count could be anything
-- below its bound), so the build side must NOT be swapped: `big` stays the build side as
-- written, even though `small` is the smaller table. Guards against an unsound bound-vs-bound swap.
SELECT * FROM small JOIN big ON small.k = big.k WHERE small.k != -1
SETTINGS log_comment = '04337_join_choose_build_table_both_bounds' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 900000 AND 1100000, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 900 AND 1100, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 1000, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04337_join_choose_build_table_both_bounds'
ORDER BY event_time DESC
LIMIT 1;

-- Soundness: a `LIMIT` (or `ORDER BY ... LIMIT`) above an inexact child yields only an upper
-- bound on the row count, which must NOT be treated as an exact estimate. The right input below
-- is a subquery with a huge `LIMIT` over a residual-filtered 10-row table: its true size is ~10,
-- but the `LIMIT` value (1000000) must not enter the exact-estimate slot. Otherwise the optimizer
-- would compare the left input's upper bound (100000) against 1000000 and wrongly swap the
-- 100000-row left input onto the build side. With the `LIMIT` kept as an upper bound, both inputs
-- are upper-bound-only, so no swap happens and the tiny right subquery stays the build side.
DROP TABLE IF EXISTS lhs_big;
DROP TABLE IF EXISTS rhs_tiny;

CREATE TABLE lhs_big (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO lhs_big SELECT number, number FROM numbers(100000);

CREATE TABLE rhs_tiny (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO rhs_tiny SELECT number FROM numbers(10);

SELECT * FROM lhs_big JOIN (SELECT k FROM rhs_tiny WHERE k != -1 ORDER BY k LIMIT 1000000) AS rr
    ON lhs_big.k = rr.k WHERE lhs_big.v != -1
SETTINGS log_comment = '04337_join_choose_build_table_limit_upper_bound' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1 AND 100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 90000 AND 110000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 10, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04337_join_choose_build_table_limit_upper_bound'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;
DROP TABLE IF EXISTS lhs_big;
DROP TABLE IF EXISTS rhs_tiny;
