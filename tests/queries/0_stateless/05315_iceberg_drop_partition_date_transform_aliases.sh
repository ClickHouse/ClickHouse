#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# `ALTER TABLE ... DROP PARTITION tuple(<transform>(<value>))` on an Iceberg table drops the partition
# that `<value>` is stored in, also when the transform is spelled `toRelativeDayNum`, `toRelativeHourNum`,
# `toMonthNumSinceEpoch` or `toYearNumSinceEpoch` (the `PARTITION BY` aliases of the Iceberg
# `day`/`hour`/`month`/`year` transforms) and the value is before 1970 or carries a non-UTC time zone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An Iceberg `timestamp` column is read back as `DateTime64(6)` in the session time zone, so the timestamps
# are inserted as explicit UTC instants.
PATHS=()
trap 'rm -rf "${PATHS[@]}" 2>/dev/null' EXIT

create_table()
{
    local table=$1 key_type=$2 partition_by=$3 values=$4
    local path="${USER_FILES_PATH}/${table}/"
    PATHS+=("${path}")
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (k ${key_type}, v String)
        ENGINE = IcebergLocal('${path}', 'Parquet')
        PARTITION BY ${partition_by}
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${table} VALUES ${values}"
}

survivors()
{
    local table=$1 name=$2
    ${CLICKHOUSE_CLIENT} --query "SELECT '${name}', groupArray(v) FROM (SELECT v FROM ${table} ORDER BY k)"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
}

drop_and_show()
{
    local name=$1 key_type=$2 partition_by=$3 values=$4 partition=$5
    local table="t_${CLICKHOUSE_DATABASE}_${RANDOM}_${name}"
    create_table "${table}" "${key_type}" "${partition_by}" "${values}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${table} DROP PARTITION ${partition}"
    survivors "${table}" "${name}"
}

drop_and_show day "Date32" "toRelativeDayNum(k)" \
    "('1969-12-25', 'pre'), ('1970-01-01', 'epoch'), ('1970-01-05', 'post')" \
    "tuple(toRelativeDayNum(toDate32('1969-12-25')))"

# 2024-01-20 02:00:00 Asia/Kolkata is 2024-01-19 20:30:00 UTC.
drop_and_show day_zoned "DateTime64(6, 'UTC')" "toRelativeDayNum(k)" \
    "(toDateTime64('2024-01-19 20:30:00', 6, 'UTC'), 'jan19'), (toDateTime64('2024-01-20 10:00:00', 6, 'UTC'), 'jan20')" \
    "tuple(toRelativeDayNum(toDateTime64('2024-01-20 02:00:00', 6, 'Asia/Kolkata')))"

drop_and_show hour "DateTime64(6, 'UTC')" "toRelativeHourNum(k)" \
    "(toDateTime64('1969-12-31 22:00:00', 6, 'UTC'), 'pre'), (toDateTime64('1970-01-01 22:00:00', 6, 'UTC'), 'post')" \
    "tuple(toRelativeHourNum(toDateTime64('1969-12-31 22:00:00', 6, 'UTC')))"

drop_and_show month "Date32" "toMonthNumSinceEpoch(k)" \
    "('1969-11-15', 'pre'), ('1970-01-15', 'post')" \
    "tuple(toMonthNumSinceEpoch(toDate32('1969-11-15')))"

drop_and_show year "Date32" "toYearNumSinceEpoch(k)" \
    "('1969-06-01', 'pre'), ('1970-06-01', 'post')" \
    "tuple(toYearNumSinceEpoch(toDate32('1969-06-01')))"

# The Iceberg `day` transform has no time zone argument, so a local-day value is rejected.
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_day_tz_argument"
create_table "${TABLE}" "DateTime64(6, 'UTC')" "toRelativeDayNum(k)" \
    "(toDateTime64('2024-01-19 20:30:00', 6, 'UTC'), 'jan19'), (toDateTime64('2024-01-20 10:00:00', 6, 'UTC'), 'jan20')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE ${TABLE} DROP PARTITION tuple(toRelativeDayNum(toDateTime64('2024-01-20 02:00:00', 6, 'UTC'), 'Asia/Kolkata'))
" 2>&1 | grep -o -m1 'NUMBER_OF_ARGUMENTS_DOESNT_MATCH'
survivors "${TABLE}" day_tz_argument
