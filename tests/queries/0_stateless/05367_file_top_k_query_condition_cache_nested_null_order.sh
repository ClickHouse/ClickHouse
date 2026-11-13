#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and ICU

# A rerun of `ORDER BY ... LIMIT n` over a Parquet `File` table reuses the TopK query condition cache entries
# of the first run, which record the row groups whose best value sorts beyond the final threshold. That
# verdict must order a NULL nested in the sort key by NULLS FIRST/LAST and strings by the collation, as the
# sort does, or a row group holding result rows is skipped on the rerun.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05367"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"

# Row group 0 = rows 0..999, row group 1 = rows 1000..1999, row group 2 = rows 2000..2999. Row group 2 holds no
# row of any result below, so the first run records it and a rerun skips it.
function create_table()
{
    local name=$1
    local type=$2
    local value=$3
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${DATA_DIR}/${name}.parquet')
        SELECT CAST(${value}, '${type}') AS s FROM numbers(3000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
    # Backdate the file so its version token has settled and the cache engages.
    touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/${name}.parquet"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_05367_${name}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05367_${name} (s ${type}) ENGINE = File(Parquet, '${DATA_DIR}/${name}.parquet')"
}

# Row group 0 holds (1, 5) and (1000000, NULL), row group 1 holds (0, 0), (1, NULL), (1000000, 7) and (1000000, 3).
create_table tuple 'Tuple(a UInt64, b Nullable(UInt64))' "multiIf(
    number = 0, (1, 5), number = 1, (1000000, NULL), number < 1000, (100 + number, number),
    number = 1000, (0, 0), number = 1001, (1, NULL), number = 1002, (1000000, 7), number = 1003, (1000000, 3),
    (200 + number, number))"
create_table array 'Array(Nullable(UInt64))' "multiIf(
    number = 0, [1, 5], number = 1, [1000000, NULL], number < 1000, [100 + number, number],
    number = 1000, [0, 0], number = 1001, [1, NULL], number = 1002, [1000000, 7], number = 1003, [1000000, 3],
    [200 + number, number])"
# Row group 0 holds 'a', row group 1 holds 'B' and 'C': byte order puts both before 'a', the collation after it.
create_table collation 'Tuple(s String)' "tuple(multiIf(
    number = 0, 'a', number < 1000, concat('x', toString(number)),
    number = 1000, 'B', number = 1001, 'C', concat('y', toString(number))))"

SETTINGS="use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 1000"

function check()
{
    local query=$1
    echo "${query}"
    for run in 1 2
    do
        echo "--- run ${run}"
        ${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${SETTINGS}, use_query_condition_cache = 1"
    done
    echo "--- no cache"
    ${CLICKHOUSE_CLIENT} --query "${query} SETTINGS ${SETTINGS}, use_query_condition_cache = 0"
}

check "SELECT s FROM t_05367_tuple ORDER BY s LIMIT 3"
check "SELECT s FROM t_05367_tuple ORDER BY s DESC NULLS FIRST LIMIT 2"
check "SELECT s FROM t_05367_array ORDER BY s LIMIT 3"
check "SELECT s FROM t_05367_array ORDER BY s DESC NULLS FIRST LIMIT 2"
check "SELECT s FROM t_05367_collation ORDER BY s COLLATE 'en' LIMIT 2"

for name in tuple array collation
do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05367_${name}"
done
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
