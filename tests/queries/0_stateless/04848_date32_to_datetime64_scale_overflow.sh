#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DateTime64` ticks are stored in an Int64, so the representable range shrinks as the scale grows:
# at scale 9 it ends at 2262-04-11 and starts at 1677-09-21, far inside the `Date32` range.
# A `Date32` value outside that window must not silently come back as the clamped boundary when
# `date_time_overflow_behavior = 'throw'`.

# Every query that is expected to fail runs in a single `clickhouse-local`, and so does every query that is
# expected to succeed: formatting an exception message symbolizes the stack trace, which costs tens of seconds
# in a sanitizer build and is paid once per process, so a process per query makes the test time out there.

echo "cast"
${CLICKHOUSE_LOCAL} --ignore-error -q "
    select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw';
    select cast(toDate32('0000-01-01') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw';
" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"

${CLICKHOUSE_LOCAL} -q "
    select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'saturate';
    -- The default ('ignore') keeps clamping rather than failing with \`DECIMAL_OVERFLOW\`.
    select cast(toDate32('9999-12-31') as DateTime64(9, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(9, 'UTC'));
    -- The scale-9 boundary days themselves are exact, and a lower scale covers the whole \`Date32\` range.
    select cast(toDate32('2262-04-11') as DateTime64(9, 'UTC')), cast(toDate32('1677-09-22') as DateTime64(9, 'UTC')) settings date_time_overflow_behavior = 'throw';
    select cast(toDate32('9999-12-31') as DateTime64(3, 'UTC')), cast(toDate32('0000-01-01') as DateTime64(3, 'UTC')) settings date_time_overflow_behavior = 'throw';
"

FORMATS=(Parquet Arrow ArrowStream ORC Avro)

for format in "${FORMATS[@]}"
do
    ${CLICKHOUSE_LOCAL} -q "insert into function file('${CLICKHOUSE_TMP}/04848_date.$format', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('9999-12-31'), ('0000-01-01')"
done

# A day whose midnight is not representable at scale 9 must be rejected by every format reader, not clamped.
echo "out of range at scale 9"
QUERIES=""
for format in "${FORMATS[@]}"
do
    QUERIES+="select * from file('${CLICKHOUSE_TMP}/04848_date.$format', $format, 'date DateTime64(9, \'UTC\')');"
done
${CLICKHOUSE_LOCAL} --ignore-error -q "$QUERIES" 2>&1 | grep -c "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"

for format in "${FORMATS[@]}"
do
    echo "$format"
    FILE=${CLICKHOUSE_TMP}/04848_date.$format

    ${CLICKHOUSE_LOCAL} -q "
        select * from file('$FILE', $format, 'date DateTime64(9, \'UTC\')') settings date_time_overflow_behavior = 'saturate';
        -- Scale 3 represents the whole \`Date32\` range, so nothing is rejected there.
        select * from file('$FILE', $format, 'date DateTime64(3, \'UTC\')');
        -- The scale-9 boundary days round-trip exactly.
        insert into function file('$FILE', $format, 'date Date32') settings engine_file_truncate_on_insert = 1 values ('1677-09-22'), ('2262-04-11');
        select * from file('$FILE', $format, 'date DateTime64(9, \'UTC\')');
    "

    rm -f "$FILE"
done
