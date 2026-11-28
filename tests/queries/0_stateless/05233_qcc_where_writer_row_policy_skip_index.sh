#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: drops the (instance-wide) query condition cache
# no-parallel-replicas: the query condition cache is populated per replica, so a hit is
#                       deterministic only on a single replica

# Companion of 03229_query_condition_cache_row_policy.sh and
# 04275_104781_qcc_prewhere_skip_index_attribution.sql for the *other* writer of the query
# condition cache: the residual `WHERE` one, where `MergeTreeSelectProcessor::readCurrentTask`
# attaches `MarkRangesInfo` and `FilterTransform` records a mark range as non-matching once the
# filter empties it. That entry is keyed on the whole `filter_actions_dag` hash rather than on a
# PREWHERE sub-predicate, so - unlike the PREWHERE writer, which this PR gates - it needs no extra
# guard against rows or marks disappearing ahead of the filter. The two ways that can happen are
# pinned here so the test reds if either stops holding:
#
#   * a row policy is ANDed into `filter_actions_dag` (`optimizePrimaryKeyConditionAndLimit` calls
#     `addFilter(getRowLevelFilter()->actions, ...)` before the filter walk), so a restricted read
#     keys on a different condition than an unrestricted one and the two never share an entry;
#   * `use_skip_indexes_on_data_read = 1` puts a skip-index reader ahead of PREWHERE in the readers
#     chain, and `MergeTreeSelectProcessor` then attaches no `MarkRangesInfo` at all (the fix for
#     issue #104781), so such a read writes nothing for a later read to pick up.
#
# Settings pinned per query so CI randomization cannot dissolve the shape:
#   * `optimize_use_implicit_projections = 0` - `_exact_count_projection` answers `count()` from
#     part metadata and bypasses the cache entirely, making every assertion vacuous.
#   * `optimize_move_to_prewhere = 0`         - keeps the predicate a residual `WHERE`, i.e. on the
#     writer under test rather than on the PREWHERE one.
#   * `max_rows_to_read = 0`                  - the CI test profile sets it to 20000000, and
#     `supportsSkipIndexesOnDataRead` bails out on a row limit with `read_overflow_mode = throw`,
#     which would make `use_skip_indexes_on_data_read = 1` below a silent no-op.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PINS="use_query_condition_cache = 1, optimize_use_implicit_projections = 0, optimize_move_to_prewhere = 0, max_rows_to_read = 0"

# User and policy names are server-global, suffix them with the (unique) test database.
user_hide="u_hide_${CLICKHOUSE_DATABASE}"
user_show="u_show_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_hide}, ${user_show}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_where;
CREATE TABLE t_qcc_where (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_qcc_where SELECT number, number FROM numbers(100);

CREATE USER ${user_hide} NOT IDENTIFIED;
CREATE USER ${user_show} NOT IDENTIFIED;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_qcc_where TO ${user_hide}, ${user_show};

-- Both users get a policy so neither trips \`throw_on_unmatched_row_policies\`, which the test
-- config turns on: ${user_hide} sees only v < 50, ${user_show} sees every row.
CREATE ROW POLICY rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_where FOR SELECT USING v < 50 TO ${user_hide};
CREATE ROW POLICY rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_where FOR SELECT USING 1 TO ${user_show};

SYSTEM DROP QUERY CONDITION CACHE;
"

# The policy of ${user_hide} hides every row that \`v >= 50\` matches, so the residual WHERE empties
# all 100 marks - recorded under the *restricted* condition, which has the policy ANDed in.
echo "-- row policy: the restricted read empties every mark (0)"
${CLICKHOUSE_CLIENT} --user "${user_hide}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_where WHERE v >= 50 SETTINGS ${PINS}"

echo "-- row policy: and it did write an entry (1)"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.query_condition_cache"

echo "-- row policy: the unrestricted read of the same query text is unaffected (50, 50)"
${CLICKHOUSE_CLIENT} --user "${user_show}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_where WHERE v >= 50 SETTINGS ${PINS}"
${CLICKHOUSE_CLIENT} --user "${user_show}" --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_where WHERE v >= 50 SETTINGS use_query_condition_cache = 0, optimize_use_implicit_projections = 0, optimize_move_to_prewhere = 0"

# Checked with the default user: ${user_show} has no \`SYSTEM FLUSH LOGS\` grant.
echo "-- row policy: because it keyed on a different condition and found nothing to reuse (0)"
${CLICKHOUSE_CLIENT} --multiquery --query "
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['QueryConditionCacheHits'])
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND user = '${user_show}';
"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP ROW POLICY rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_where;
DROP ROW POLICY rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_where;
DROP USER ${user_hide}, ${user_show};
DROP TABLE t_qcc_where;
"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_where_idx;
CREATE TABLE t_qcc_where_idx (id UInt64, v UInt64, w UInt64, INDEX idx_w w TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
-- \`w\` runs against \`id\`, so a minmax granule covers a narrow, prunable range; \`v\` is the part of
-- the condition the index cannot answer, which is what empties the surviving granules.
INSERT INTO t_qcc_where_idx SELECT number, number % 7, number FROM numbers(500000);
"

echo "-- skip index on data read: the read writes no entry at all (14, 0)"
${CLICKHOUSE_CLIENT} --multiquery --query "
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS ${PINS}, use_skip_indexes_on_data_read = 1;
SELECT count() FROM system.query_condition_cache;
"

echo "-- skip index on data read: so a later indexes-off read cannot be poisoned (14, 14)"
${CLICKHOUSE_CLIENT} --multiquery --query "
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS ${PINS}, use_skip_indexes = 0, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS use_query_condition_cache = 0, optimize_use_implicit_projections = 0, optimize_move_to_prewhere = 0, use_skip_indexes = 0;
"

# Liveness: without the data-read pruning the same shape *does* write, and the entry is reused both
# with the indexes off and back on - so the zero above is the guard, not a dead test.
echo "-- without the data-read pruning the same shape writes and is reused (14, 1, 14, 14)"
${CLICKHOUSE_CLIENT} --multiquery --query "
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS ${PINS}, use_skip_indexes = 0, use_skip_indexes_on_data_read = 0;
SELECT count() FROM system.query_condition_cache;
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS ${PINS}, use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, log_comment = '05233_reuse';
SELECT count() FROM t_qcc_where_idx WHERE v = 3 AND w < 100
SETTINGS ${PINS}, use_skip_indexes_on_data_read = 1;
"

echo "-- and that reuse really was a cache hit (1)"
${CLICKHOUSE_CLIENT} --multiquery --query "
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05233_reuse'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_qcc_where_idx;
"
