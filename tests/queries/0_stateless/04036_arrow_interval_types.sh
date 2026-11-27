#!/usr/bin/env bash
# Tags: no-fasttest
# ^ uses the Arrow library, which is not available under fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that all interval kinds can be exported to Arrow/ArrowStream and values are preserved
tmp_dir="${CUR_DIR}/tmp/04036_arrow_interval_types/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${tmp_dir}"
cleanup()
{
    rm -f "${tmp_dir}/arrow_"*.bin
}
trap cleanup EXIT

for fmt in Arrow ArrowStream; do
    echo "=== $fmt ==="
    out_file="${tmp_dir}/arrow_${fmt}.bin"
    ${CLICKHOUSE_LOCAL} -q "
        SELECT
            3::IntervalNanosecond  AS ns,
            4::IntervalMicrosecond AS us,
            5::IntervalMillisecond AS ms,
            6::IntervalSecond      AS s,
            7::IntervalMinute      AS m,
            8::IntervalHour        AS h,
            9::IntervalDay         AS d,
            10::IntervalWeek       AS w,
            11::IntervalMonth      AS mo,
            12::IntervalQuarter    AS q,
            13::IntervalYear       AS y
        FORMAT $fmt
    " > "${out_file}"

    echo "-- inferred types --"
    ${CLICKHOUSE_LOCAL} -q "
        SELECT
            toTypeName(ns) AS ns,
            toTypeName(us) AS us,
            toTypeName(ms) AS ms,
            toTypeName(s)  AS s,
            toTypeName(m)  AS m,
            toTypeName(h)  AS h,
            toTypeName(d)  AS d,
            toTypeName(w)  AS w,
            toTypeName(mo) AS mo,
            toTypeName(q)  AS q,
            toTypeName(y)  AS y
        FROM file('${out_file}', '$fmt')
    "

    echo "-- values --"
    ${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${out_file}', '$fmt')"
done
