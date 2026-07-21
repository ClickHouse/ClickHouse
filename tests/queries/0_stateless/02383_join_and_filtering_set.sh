#!/usr/bin/env bash
# Tags: no-asan,no-msan,no-tsan,no-ubsan
#
# Test doesn't run complex queries, just test the logic of setting, so no need to run with different builds.
# Also, we run similar queries in 02382_join_and_filtering_set.sql which is enabled for these builds.
#

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q """
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY y
AS SELECT sipHash64(number, 't1_x') % 100 AS x, sipHash64(number, 't1_y') % 100 AS y FROM numbers(100);

CREATE TABLE t2 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY y
AS SELECT sipHash64(number, 't2_x') % 100 AS x, sipHash64(number, 't2_y') % 100 AS y FROM numbers(100);
"""

# Arguments:
# - Query result
# - Processor name
# - Expected description
# - Check first occurrence
function match_description() {

QUERY_RESULT=$1
PROCESSOR_NAME=$2
EXPECTED_DESCRIPTION=$3
CHECK_FIRST_OCCURRENCE=${4:-true}

SED_EXPR="/$PROCESSOR_NAME/{ n; s/^[ \t]*Description: //; p"
[ $CHECK_FIRST_OCCURRENCE = true ] && SED_EXPR+="; q }" || SED_EXPR+=" }"

DESC=$(sed -n "$SED_EXPR" <<<  "$QUERY_RESULT")
[[ "$DESC" == "$EXPECTED_DESCRIPTION" ]] && echo "Ok" || echo "Fail: ReadHeadBalancedProcessor description '$DESC' != '$EXPECTED_DESCRIPTION' "

}

# Arguments:
# - value of max_rows_in_set_to_optimize_join
# - join kind
# - expected number of steps in plan
# - expected number of steps in pipeline
function test() {

PARAM_VALUE=$1
JOIN_KIND=${2:-}

EXPECTED_PLAN_STEPS=$3
RES=$(
    $CLICKHOUSE_CLIENT --max_rows_in_set_to_optimize_join=${PARAM_VALUE} --join_algorithm='full_sorting_merge' \
                       -q "EXPLAIN PLAN SELECT count() FROM t1 ${JOIN_KIND} JOIN t2 ON t1.x = t2.x" | grep -o 'CreateSetAndFilterOnTheFlyStep' | wc -l
)
[ "$RES" -eq "$EXPECTED_PLAN_STEPS" ] && echo "Ok" || echo "Fail: $RES != $EXPECTED_PLAN_STEPS"

EXPECTED_PIPELINE_STEPS=$4
RES=$(
    $CLICKHOUSE_CLIENT --max_rows_in_set_to_optimize_join=${PARAM_VALUE} --join_algorithm='full_sorting_merge' \
                       -q "EXPLAIN PIPELINE SELECT count() FROM t1 ${JOIN_KIND} JOIN t2 ON t1.x = t2.x"
)

# Count match
COUNT=$(echo "$RES" | grep -o -e ReadHeadBalancedProcessor -e FilterBySetOnTheFlyTransform -e CreatingSetsOnTheFlyTransform | wc -l)
[ "$COUNT" -eq "$EXPECTED_PIPELINE_STEPS" ] && echo "Ok" || echo "Fail: $COUNT != $EXPECTED_PIPELINE_STEPS"

# Description matchers
if [ "$EXPECTED_PIPELINE_STEPS" -ne 0 ]; then
    match_description "$RES" 'ReadHeadBalancedProcessor' 'Reads rows from two streams evenly'
    match_description "$RES" 'FilterBySetOnTheFlyTransform' "Filter rows using other join table side\'s set"
    match_description "$RES" 'CreatingSetsOnTheFlyTransform' 'Create set and filter Left joined stream
Create set and filter Right joined stream' false
fi

}

test 1000 '' 2 6

# no filtering for left/right side
test 1000 'LEFT' 2 5
test 1000 'RIGHT' 2 5

# when disabled no extra steps should be created
test 1000 'FULL' 0 0
test 0 '' 0 0
