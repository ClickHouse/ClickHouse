#!/usr/bin/env bash
# Tags: no-fasttest, race

# Test for race condition in Parquet reader between:
# - Thread assigning to next_subgroup_for_step in intersectColumnIndexResultsAndInitSubgroups
# - Thread reading next_subgroup_for_step.empty() in scheduleTasksIfNeeded
# This race is more likely to be caught with ThreadSanitizer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_race_test.parquet"

# Create a parquet file with many small row groups to maximize parallelism
# and increase the chance of hitting the race condition.
${CLICKHOUSE_LOCAL} -q "
    INSERT INTO FUNCTION file('${FILE_PATH}')
    SELECT number, randomString(100) as s
    FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000
"

# Read the file multiple times in parallel to trigger the race.
# The race happens when multiple row groups are processed concurrently
# and scheduleTasksIfNeeded checks is_privileged_task while another
# thread is initializing next_subgroup_for_step.
for _ in {1..20}; do
    ${CLICKHOUSE_LOCAL} -q "
        SELECT count()
        FROM file('${FILE_PATH}')
        SETTINGS
            input_format_parquet_use_native_reader_v3 = 1,
            input_format_parquet_preserve_order = 0,
            max_threads = 8
    "
done

# Cleanup
rm -f "${FILE_PATH}"
