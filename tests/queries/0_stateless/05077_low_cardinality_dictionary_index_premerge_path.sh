#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_index_premerge_path"
TRACE_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_premerge.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${TRACE_FILE}"' EXIT
CLICKHOUSE_CLIENT_TRACE=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace/g")

${CLICKHOUSE_CLIENT} -q "
    SET max_threads = 1, max_insert_threads = 1, max_block_size = 3, max_insert_block_size = 3;

    DROP TABLE IF EXISTS ${TABLE_NAME};
    CREATE TABLE ${TABLE_NAME}
    (
        id UInt64,
        key LowCardinality(String),
        value UInt64
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS
        index_granularity = 1,
        index_granularity_bytes = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_wide_part = 0,
        min_level_for_wide_part = 0;

    SYSTEM STOP MERGES ${TABLE_NAME};

    INSERT INTO ${TABLE_NAME} VALUES (0, 'shared', 10), (2, 'shared', 30);
    INSERT INTO ${TABLE_NAME} VALUES (1, 'shared', 20);

    SELECT throwIf(
        count() != 2 OR countIf(part_type = 'Wide') != 2
            OR countIf(rows = 2) != 1 OR countIf(rows = 1) != 1,
        'Expected two Wide parts with interleaved rows for dictionary pre-merging')
    FROM system.parts
    WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
    FORMAT Null;
"

# Sorting one-row blocks by `id` makes the single producer see dictionaries A, B, A.
# The initial local state and the later shared state for A must pre-merge: this does
# not depend on parallel read scheduling. `groupArraySorted` also exercises a non-trivial
# aggregate state; `groupArray` is an order-dependent control that must not pre-merge.
run_aggregation()
{
    local aggregate="$1"
    local two_level_threshold="$2"

    # A high external-aggregation threshold permits two-level tables with one input stream,
    # without actually spilling this three-row query. Disable the ratio and size hints so
    # neither randomized settings nor an earlier run chooses a different aggregation path.
    ${CLICKHOUSE_CLIENT_TRACE} -q "
        SELECT key, count(), sum(value), ${aggregate}(value)
        FROM
        (
            SELECT key, value
            FROM ${TABLE_NAME}
            ORDER BY id
        )
        GROUP BY key
        ORDER BY key
        SETTINGS
            max_threads = 1,
            max_block_size = 1,
            preferred_block_size_bytes = 0,
            optimize_read_in_order = 1,
            query_plan_remove_redundant_sorting = 0,
            optimize_aggregation_in_order = 0,
            enable_adaptive_aggregator = 0,
            collect_hash_table_stats_during_aggregation = 0,
            max_rows_to_group_by = 0,
            max_bytes_before_external_group_by = 1073741824,
            max_bytes_ratio_before_external_group_by = 0,
            group_by_two_level_threshold = ${two_level_threshold},
            group_by_two_level_threshold_bytes = 0;
    " 2>"${TRACE_FILE}" || {
        query_status=$?
        cat "${TRACE_FILE}" >&2
        return "$query_status"
    }

    # Keep the exact variant/group/key counts, stripping only worker count and timing.
    # Missing, additional, or differently sized pre-merges all differ from the reference.
    awk '
        /Aggregator: Converting aggregation data to two-level\./ { two_level = 1 }
        /Aggregator: Merged [0-9]+ single-dictionary variants in [0-9]+ dictionary groups by index into [0-9]+ variants:/ {
            sub(/^.*Aggregator: /, "")
            sub(/, [0-9]+ workers,.*$/, "")
            print
        }
        END { print "two-level aggregation: " (two_level ? 1 : 0) }
    ' "${TRACE_FILE}"
}

for two_level_threshold in 0 1; do
    echo "order-independent, two-level threshold ${two_level_threshold}"
    run_aggregation 'groupArraySorted(10)' "${two_level_threshold}"

    echo "order-dependent, two-level threshold ${two_level_threshold}"
    run_aggregation 'groupArray' "${two_level_threshold}"
done
