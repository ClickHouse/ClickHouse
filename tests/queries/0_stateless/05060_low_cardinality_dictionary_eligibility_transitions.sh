#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE_NAME="low_cardinality_dictionary_eligibility_transitions"
TRACE_FILE=$(mktemp "${CLICKHOUSE_TMP}/dictionary_eligibility.XXXXXX")
trap '${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"; rm -f "${TRACE_FILE}"' EXIT
CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT//"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/--send_logs_level=trace}

for dictionary_sequence in different same; do
    setup_queries="
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
            index_granularity_bytes = '10Mi',
            min_rows_for_wide_part = 0,
            min_bytes_for_wide_part = 0,
            min_level_for_wide_part = 0;

        SYSTEM STOP MERGES ${TABLE_NAME};
    "

    if [[ "$dictionary_sequence" == different ]]; then
        setup_queries+="
            INSERT INTO ${TABLE_NAME}
            VALUES (0, 'shared', 10);

            INSERT INTO ${TABLE_NAME}
            VALUES (2, 'shared', 30);
        "
    else
        setup_queries+="
            INSERT INTO ${TABLE_NAME}
            VALUES (0, 'shared', 10), (2, 'shared', 30);
        "
    fi

    ${CLICKHOUSE_CLIENT} -q "${setup_queries}
        ALTER TABLE ${TABLE_NAME} MODIFY SETTING min_rows_for_wide_part = 2;

        INSERT INTO ${TABLE_NAME}
        VALUES (1, 'other', 20);

        ALTER TABLE ${TABLE_NAME} MODIFY SETTING min_rows_for_wide_part = 0;

        SELECT throwIf(
            countIf(part_type = 'Compact') != 1
                OR countIf(part_type = 'Wide') != if('${dictionary_sequence}' = 'different', 2, 1)
                OR sumIf(rows, part_type = 'Compact') != 1
                OR sumIf(rows, part_type = 'Wide') != 2,
            'Unexpected part layout for dictionary eligibility transitions')
        FROM system.parts
        WHERE database = currentDatabase() AND table = '${TABLE_NAME}' AND active
        FORMAT Null;
    "

    for two_level_threshold in 0 1; do
        echo "${dictionary_sequence} dictionary, two-level threshold ${two_level_threshold}"
        ${CLICKHOUSE_CLIENT_TRACE} -q "
            SELECT key, count(), sum(value)
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
                max_rows_to_group_by = 0,
                group_by_two_level_threshold = ${two_level_threshold},
                group_by_two_level_threshold_bytes = 0;
        " 2>"${TRACE_FILE}" || {
            query_status=$?
            cat "${TRACE_FILE}" >&2
            exit "$query_status"
        }

        awk '
            /Aggregator: Aggregation method: low_cardinality_single_dictionary/ { print "index aggregation" }
            /Aggregator: Aggregation method normalized to:/ { print "value aggregation" }
        ' "${TRACE_FILE}"
    done
done
