#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_suffix="${CLICKHOUSE_TEST_UNIQUE_NAME//[^[:alnum:]_]/_}"
table="cache_incomplete_${test_suffix}"
dim="cache_dim_${test_suffix}"
query_prefix="${test_suffix}_$$_${RANDOM}_${RANDOM}"
# Disable query-condition pruning: statistics-cache checks need all fixture parts.
common_settings="--allow_statistics=1 --enable_analyzer=1 --explain_query_plan_default=legacy --use_statistics=1 --use_statistics_for_part_pruning=0 --use_query_condition_cache=0 --enable_cascades_optimizer=0 --enable_parallel_replicas=0 --enable_join_runtime_filters=0 --query_plan_optimize_join_order_limit=10 --query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=0 --use_hash_table_stats_for_join_reordering=0 --mutations_sync=2 --alter_sync=2 --log_queries=1 --max_threads=1"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${dim} SYNC; DROP TABLE IF EXISTS ${table} SYNC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

die()
{
    echo "FAIL: $*" >&2
    exit 1
}

# A missing log row is never a cache hit: require exactly one QueryFinish for its unique ID.
loaded_event()
{
    local query_id="$1"
    local row_count loaded row

    for attempt in $(seq 1 3); do
        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log" >/dev/null
        row=$($CLICKHOUSE_CLIENT --enable_parallel_replicas=0 -q "
            SELECT count(), any(ProfileEvents['LoadedStatisticsMicroseconds'])
            FROM system.query_log
            WHERE current_database = currentDatabase()
                AND type = 'QueryFinish'
                AND query_id = '${query_id}'
            FORMAT TabSeparated")
        IFS=$'\t' read -r row_count loaded <<< "$row"

        if [[ "$row_count" == "1" ]]; then
            printf '%s\n' "$loaded"
            return 0
        fi
        if [[ "$row_count" -gt 1 ]]; then
            die "query_id=${query_id}: expected one QueryFinish row, found ${row_count}"
        fi
        sleep 0.1
    done

    die "query_id=${query_id}: no QueryFinish row after three bounded flush polls"
}

run_and_read_event()
{
    local query_id="$1"
    local use_cache="$2"
    local query="$3"
    local expected_result="$4"
    local result

    # shellcheck disable=SC2086
    result=$($CLICKHOUSE_CLIENT $common_settings --use_statistics_cache="$use_cache" --query_id="$query_id" -q "$query")
    [[ "$result" == "$expected_result" ]] || die "query_id=${query_id}: expected result ${expected_result}, got ${result}"
    loaded_event "$query_id"
}

expect_positive_load()
{
    local label="$1"
    local loaded="$2"
    [[ "$loaded" =~ ^[0-9]+$ ]] || die "$label: non-numeric LoadedStatisticsMicroseconds=${loaded}"
    (( loaded > 0 )) || die "$label: expected a positive cold statistics load, got ${loaded}"
}

wait_for_full_cache_hit()
{
    local phase="$1"
    local query="$2"
    local expected_result="$3"
    local loaded query_id
    local zero_streak=0

    for attempt in $(seq 1 40); do
        query_id="${query_prefix}_${phase}_${attempt}"
        loaded=$(run_and_read_event "$query_id" 1 "$query" "$expected_result")
        [[ "$loaded" =~ ^[0-9]+$ ]] || die "$phase: non-numeric LoadedStatisticsMicroseconds=${loaded}"
        if [[ "$loaded" == "0" ]]; then
            (( zero_streak += 1 ))
            if (( zero_streak >= 3 )); then
                echo "$phase: three distinct QueryFinish rows with zero loader time"
                return 0
            fi
        else
            zero_streak=0
        fi
        sleep 0.25
    done

    die "$phase: no three consecutive zero-loader full-part queries within 40 attempts"
}

part_statistics()
{
    $CLICKHOUSE_CLIENT --enable_parallel_replicas=0 -q "
        SELECT partition, column, notEmpty(statistics)
        FROM system.parts_columns
        WHERE database = currentDatabase()
            AND table = '${table}'
            AND active
            AND column IN ('v', 'y')
        ORDER BY partition, column
        FORMAT TabSeparated"
}

nullable_part_statistics()
{
    $CLICKHOUSE_CLIENT --enable_parallel_replicas=0 -q "
        SELECT partition, column, notEmpty(statistics)
        FROM system.parts_columns
        WHERE database = currentDatabase()
            AND table = '${table}'
            AND active
            AND column = 'n'
        ORDER BY partition, column
        FORMAT TabSeparated"
}

plan_relation_token()
{
    local use_cache="$1"
    local predicate="$2"
    local select_settings="${3:-}"
    local token token_regex

    # The exact empty estimator can legitimately yield an unknown relation row
    # estimate. Compare the full table[...] token instead of assuming digits.
    # shellcheck disable=SC2086
    token=$($CLICKHOUSE_CLIENT $common_settings --use_statistics_cache="$use_cache" -q "
        SELECT extract(explain, '(${table}\\[[^]]+\\])')
        FROM
        (
            EXPLAIN keep_logical_steps = 1, actions = 1
            SELECT count()
            FROM ${table}
            INNER JOIN ${dim} ON ${dim}.id = ${table}.id
            WHERE ${predicate}
            ${select_settings}
        )
        WHERE explain LIKE '%Join:%'
        FORMAT TabSeparated")

    token_regex="^${table}\\[[^]]+\\]$"
    [[ "$token" =~ $token_regex ]] || die "could not extract a nonempty ${table}[...] relation token (cache=${use_cache}, where=${predicate}): ${token}"
    printf '%s\n' "$token"
}

assert_equal_token()
{
    local label="$1"
    local cold="$2"
    local cached="$3"
    local token_regex="^${table}\\[[^]]+\\]$"
    [[ "$cold" =~ $token_regex && "$cached" =~ $token_regex ]] || die "$label: relation tokens must be nonempty"
    [[ "$cold" == "$cached" ]] || die "$label: cache-off/cache-on relation tokens differ (${cold} vs ${cached})"
}

# Two 500-row parts plus a 1,000-row dimension. Only the subject MergeTree has
# statistics; this isolates its loader event from any join-side load.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $common_settings --materialize_statistics_on_insert=1 -q "
    DROP TABLE IF EXISTS ${table} SYNC;
    DROP TABLE IF EXISTS ${dim} SYNC;
    CREATE TABLE ${table}
    (
        p UInt8,
        id UInt64,
        v UInt64 STATISTICS(basic),
        y UInt8 STATISTICS(basic)
    )
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY id
    SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;
    CREATE TABLE ${dim} (id UInt64)
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;
    INSERT INTO ${table} SELECT 0, number, number, number % 2 FROM numbers(500);
    INSERT INTO ${table} SELECT 1, number + 500, number + 500, (number + 500) % 2 FROM numbers(500);
    INSERT INTO ${dim} SELECT number FROM numbers(1000);
    ALTER TABLE ${table} CLEAR STATISTICS v IN PARTITION 1;
    ALTER TABLE ${table} MODIFY SETTING refresh_statistics_interval = 1;
"

[[ "$(part_statistics)" == $'0\tv\t1\n0\ty\t1\n1\tv\t0\n1\ty\t1' ]] || die "expected incomplete v and complete y coverage, got $(part_statistics)"

unreferenced_predicate="${table}.v >= 750"
unreferenced_query="SELECT count() FROM ${table} INNER JOIN ${dim} ON ${dim}.id = ${table}.id WHERE ${unreferenced_predicate} FORMAT TabSeparated"
unreferenced_expected_result=250

unreferenced_cold_load=$(run_and_read_event "${query_prefix}_unreferenced_cold" 0 "$unreferenced_query" "$unreferenced_expected_result")
expect_positive_load "unreferenced-y cache-off control" "$unreferenced_cold_load"
unreferenced_cold_token=$(plan_relation_token 0 "$unreferenced_predicate")
wait_for_full_cache_hit "unreferenced-y partial-state snapshot" "$unreferenced_query" "$unreferenced_expected_result"
unreferenced_cached_token=$(plan_relation_token 1 "$unreferenced_predicate")
assert_equal_token "unreferenced-y partial-state cache reuse" "$unreferenced_cold_token" "$unreferenced_cached_token"

predicate="${table}.v >= 750 AND ${table}.y = 0"
query="SELECT count() FROM ${table} INNER JOIN ${dim} ON ${dim}.id = ${table}.id WHERE ${predicate} FORMAT TabSeparated"
expected_result=125

cold_load=$(run_and_read_event "${query_prefix}_partial_cold" 0 "$query" "$expected_result")
expect_positive_load "partial-state cache-off control" "$cold_load"
cold_token=$(plan_relation_token 0 "$predicate")
wait_for_full_cache_hit "partial-state full-part snapshot" "$query" "$expected_result"
cached_token=$(plan_relation_token 1 "$predicate")
assert_equal_token "partial-state cache reuse" "$cold_token" "$cached_token"

# Clear y only where v remains: a compact-part rewrite can rebuild missing v.
# The resulting complementary v-only/y-only parts have no complete column. The
# exact full-part cache entry must retain the negative marker, which the planner
# sees as nullptr, rather than treating this snapshot as absent.
# The changed part names force publication for this new active-part snapshot.
parts_before=$($CLICKHOUSE_CLIENT --enable_parallel_replicas=0 -q "
    SELECT arrayStringConcat(arraySort(groupArray(name)), ',')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${table}' AND active
    FORMAT TabSeparated")
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $common_settings -q "ALTER TABLE ${table} CLEAR STATISTICS y IN PARTITION 0"
parts_after=$($CLICKHOUSE_CLIENT --enable_parallel_replicas=0 -q "
    SELECT arrayStringConcat(arraySort(groupArray(name)), ',')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${table}' AND active
    FORMAT TabSeparated")
[[ -n "$parts_before" && "$parts_after" != "$parts_before" ]] || die "CLEAR STATISTICS y did not replace the active part snapshot"
[[ "$(part_statistics)" == $'0\tv\t1\n0\ty\t0\n1\tv\t0\n1\ty\t1' ]] || die "expected complementary incomplete v/y coverage, got $(part_statistics)"

empty_cold_load=$(run_and_read_event "${query_prefix}_empty_cold" 0 "$query" "$expected_result")
expect_positive_load "empty-state cache-off control" "$empty_cold_load"
wait_for_full_cache_hit "empty-estimator full-part snapshot" "$query" "$expected_result"
empty_cold_token=$(plan_relation_token 0 "$predicate")
empty_cached_token=$(plan_relation_token 1 "$predicate")
assert_equal_token "empty-state cache reuse" "$empty_cold_token" "$empty_cached_token"

# Reuse the fact table after the negative-cache phase to cover the stored parent
# statistic required by a Nullable .null subcolumn. Keep the dimension unchanged.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $common_settings --materialize_statistics_on_insert=1 -q "
    DROP TABLE ${table} SYNC;
    CREATE TABLE ${table}
    (
        p UInt8,
        id UInt64,
        n Nullable(UInt64) STATISTICS(basic)
    )
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY id
    SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;
    INSERT INTO ${table}
    SELECT 0, number, if(number % 5 = 0, CAST(NULL, 'Nullable(UInt64)'), number)
    FROM numbers(500);
    INSERT INTO ${table}
    SELECT 1, number + 500, if(number % 5 = 0, CAST(NULL, 'Nullable(UInt64)'), number + 500)
    FROM numbers(500);
    ALTER TABLE ${table} MODIFY SETTING refresh_statistics_interval = 1;
"

[[ "$(nullable_part_statistics)" == $'0\tn\t1\n1\tn\t1' ]] || die "expected complete parent n statistics in both parts, got $(nullable_part_statistics)"

nullable_predicate="${table}.n.null != 0"
nullable_settings="SETTINGS optimize_functions_to_subcolumns=1"
nullable_query="SELECT count() FROM ${table} INNER JOIN ${dim} ON ${dim}.id = ${table}.id WHERE ${nullable_predicate} ${nullable_settings} FORMAT TabSeparated"
nullable_expected_result=200

nullable_cold_load=$(run_and_read_event "${query_prefix}_nullable_cold" 0 "$nullable_query" "$nullable_expected_result")
expect_positive_load "nullable .null cache-off control" "$nullable_cold_load"
nullable_cold_token=$(plan_relation_token 0 "$nullable_predicate" "$nullable_settings")
wait_for_full_cache_hit "nullable-null-map snapshot" "$nullable_query" "$nullable_expected_result"
nullable_cached_token=$(plan_relation_token 1 "$nullable_predicate" "$nullable_settings")
assert_equal_token "nullable .null cache reuse" "$nullable_cold_token" "$nullable_cached_token"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${dim} SYNC; DROP TABLE ${table} SYNC"
trap - EXIT
