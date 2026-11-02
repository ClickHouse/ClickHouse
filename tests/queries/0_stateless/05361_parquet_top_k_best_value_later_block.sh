#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# A TopK read of a Parquet file records the best key of each row group for the query condition cache. Here the best
# key of a row group is not in its first block, so a rerun that uses the cache must still return the same rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05361"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
rm -f "${USER_FILES_PATH}/${DATA_DIR}"/*.parquet

# Two row groups of 10000 rows. The first block of the first row group holds only its maximum of `k` (minimum of `d`),
# its minimum of `k` (maximum of `d`) is in a later block. For `e` that first block holds the minimum and larger values,
# and the later blocks only values the TopK filter drops. The first block of the second row group holds its bounds.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet')
    SELECT toUInt64(multiIf(number < 1000, 5000, number = 5000, 1, number < 10000, 3000 + number % 1000, 100 + number % 1000)) AS k,
        toUInt64(10000 - k) AS d, toDecimal64(k, 2) AS dec, leftPad(toString(k), 4, '0') AS s,
        toUInt64(multiIf(number = 0, 1, number < 1000, 5000, number < 10000, 6000 + number % 1000, 100 + number % 1000)) AS e
    FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 10000, max_block_size = 1000, engine_file_truncate_on_insert = 1"

# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05361"
# A `File` table takes its format settings from its own `SETTINGS`, not from the query.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05361 (k UInt64, d UInt64, dec Decimal(18, 2), s String, e UInt64) ENGINE = File(Parquet, '${DATA_DIR}/data.parquet')
    SETTINGS input_format_parquet_max_block_size = 1000"

SETTINGS="use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    max_threads = 1, max_parsing_threads = 1, output_format_decimal_trailing_zeros = 0"

for query in \
    "SELECT k FROM t_05361 ORDER BY k LIMIT 3" \
    "SELECT d FROM t_05361 ORDER BY d DESC LIMIT 3" \
    "SELECT dec FROM t_05361 ORDER BY dec LIMIT 3" \
    "SELECT s FROM t_05361 ORDER BY s LIMIT 3" \
    "SELECT e FROM t_05361 ORDER BY e LIMIT 3"
do
    # The second run skips the row groups that the first one recorded as holding no row of the result.
    for _ in 1 2
    do
        ${CLICKHOUSE_CLIENT} --query "$query SETTINGS $SETTINGS" | xargs
    done
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05361"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
