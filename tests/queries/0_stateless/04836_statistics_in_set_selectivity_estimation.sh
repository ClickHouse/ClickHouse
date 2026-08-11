#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ConditionSelectivityEstimator` must bound what it spends on `col IN (set)`: it must not run a
# subquery to fill a set, and above `statistics_max_set_size_for_exact_selectivity_estimation` it
# must estimate from the size and bounds of the set instead of deriving every range.

RUN="${CLICKHOUSE_DATABASE}_$$_${RANDOM}"

# Statistics are only materialized by a merge, so two parts and an OPTIMIZE are required - with a
# single level-0 part the estimator has nothing to load and none of this code runs.
$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS probe_tbl;
DROP TABLE IF EXISTS set_tbl;
CREATE TABLE probe_tbl (k1 UInt64, k2 UInt64, payload String) ENGINE = MergeTree ORDER BY k1
SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE set_tbl (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO probe_tbl SELECT number, number, repeat('x', 20) FROM numbers(20000);
INSERT INTO probe_tbl SELECT number + 20000, number, repeat('x', 20) FROM numbers(20000);
INSERT INTO set_tbl SELECT number FROM numbers(20000);
OPTIMIZE TABLE probe_tbl FINAL;
"

echo '--- statistics are materialized (merged part, level >= 1) ---'
$CLICKHOUSE_CLIENT --query "
SELECT max(level) >= 1 FROM system.parts WHERE database = currentDatabase() AND table = 'probe_tbl' AND active;
"

QUERY="SELECT count() FROM probe_tbl WHERE k1 IN (SELECT id FROM set_tbl) AND k2 > 5"

# The 20000-element set is above the limit in the first run and below it in the second, so the same
# query takes the size-based path and then the exact-range path.
$CLICKHOUSE_CLIENT --use_statistics=1 --optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1 --allow_reorder_prewhere_conditions=1 --statistics_max_set_size_for_exact_selectivity_estimation=10000 \
    --log_comment="capped_${RUN}" --query "$QUERY FORMAT Null"
$CLICKHOUSE_CLIENT --use_statistics=1 --optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1 --allow_reorder_prewhere_conditions=1 --statistics_max_set_size_for_exact_selectivity_estimation=0 \
    --log_comment="exact_${RUN}" --query "$QUERY FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Above the limit the size-based estimate is used; with the limit disabled it never is.
echo '--- size-based estimate used only above the limit ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    sum(ProfileEvents['SelectivityEstimatorInSetEstimatedFromSize']) FILTER (WHERE log_comment = 'capped_${RUN}') > 0 AS capped_uses_size,
    sum(ProfileEvents['SelectivityEstimatorInSetEstimatedFromSize']) FILTER (WHERE log_comment = 'exact_${RUN}') = 0 AS exact_never_uses_size
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment IN ('capped_${RUN}', 'exact_${RUN}');
"

# Whichever path the estimator takes, it only ranks PREWHERE candidates - the answer cannot change.
echo '--- the estimate never changes the result ---'
$CLICKHOUSE_CLIENT -m --query "
SELECT
    (SELECT count() FROM probe_tbl WHERE k1 IN (SELECT id FROM set_tbl) AND k2 > 5
     SETTINGS use_statistics = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1, statistics_max_set_size_for_exact_selectivity_estimation = 10000) AS capped,
    capped = (SELECT count() FROM probe_tbl WHERE k1 IN (SELECT id FROM set_tbl) AND k2 > 5
              SETTINGS use_statistics = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1, statistics_max_set_size_for_exact_selectivity_estimation = 0) AS same_as_exact,
    capped = (SELECT count() FROM probe_tbl WHERE k1 IN (SELECT id FROM set_tbl) AND k2 > 5
              SETTINGS use_statistics = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1) AS same_as_no_statistics;
"

# The size-based estimate must agree with the exact ranges well enough to pick the same PREWHERE.
echo '--- same PREWHERE as the exact ranges ---'
capped_pw=$($CLICKHOUSE_CLIENT --use_statistics=1 --optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1 --allow_reorder_prewhere_conditions=1 --statistics_max_set_size_for_exact_selectivity_estimation=10000 \
    --query "EXPLAIN actions=1 $QUERY" | grep -F 'Prewhere filter column:')
exact_pw=$($CLICKHOUSE_CLIENT --use_statistics=1 --optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1 --allow_reorder_prewhere_conditions=1 --statistics_max_set_size_for_exact_selectivity_estimation=0 \
    --query "EXPLAIN actions=1 $QUERY" | grep -F 'Prewhere filter column:')
[ -n "$capped_pw" ] && echo 1 || echo "no prewhere chosen"
[ "$capped_pw" = "$exact_pw" ] && echo 1 || { echo "PREWHERE differs"; echo "$capped_pw"; echo "$exact_pw"; }

# A set nobody has built cannot be analysed, because filling it would mean running the subquery
# during planning. `k1` is not the sort key here, so no index analysis builds the set first.
echo '--- an unbuilt set is skipped rather than filled ---'
$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS probe_unindexed;
CREATE TABLE probe_unindexed (k1 UInt64, k2 UInt64, payload String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO probe_unindexed SELECT number, number, repeat('x', 20) FROM numbers(20000);
INSERT INTO probe_unindexed SELECT number + 20000, number, repeat('x', 20) FROM numbers(20000);
OPTIMIZE TABLE probe_unindexed FINAL;
"
$CLICKHOUSE_CLIENT --use_statistics=1 --optimize_move_to_prewhere=1 --query_plan_optimize_prewhere=1 --allow_reorder_prewhere_conditions=1 --log_comment="unbuilt_${RUN}" \
    --query "SELECT count() FROM probe_unindexed WHERE k1 IN (SELECT id FROM set_tbl) AND k2 > 5 FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
SELECT sum(ProfileEvents['SelectivityEstimatorInSetNotBuilt']) > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = 'unbuilt_${RUN}';
"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE probe_tbl;
DROP TABLE probe_unindexed;
DROP TABLE set_tbl;
"
