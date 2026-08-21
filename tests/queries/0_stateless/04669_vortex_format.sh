#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

echo "Round trip:"
$CLICKHOUSE_LOCAL -q "
    SELECT
        number AS n,
        toString(number) AS s,
        if(number % 2 = 0, NULL, number) AS nullable,
        range(number % 4) AS arr,
        tuple(number, toString(number)) AS t,
        toDecimal64(number, 3) / 8 AS dec,
        toDate32('2020-01-01') + number AS d,
        toDateTime64('2020-01-01 01:02:03.123456', 6, 'UTC') + number AS dt,
        toLowCardinality(toString(number % 3)) AS lc,
        toFloat64(number) / 7 AS f,
        number % 2 = 0 AS b,
        toFixedString(toString(number), 2) AS fs
    FROM numbers(10)
    FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex') FORMAT TSV"

echo "Schema inference:"
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_FILE', 'Vortex')"

echo "Count from metadata:"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', 'Vortex')"

echo "Projection:"
$CLICKHOUSE_LOCAL -q "SELECT s, n FROM file('$DATA_FILE', 'Vortex') WHERE n IN (1, 8) FORMAT TSV"

echo "Reading a subset of columns with an explicit schema and a missing column:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex', 'n UInt64, missing String') LIMIT 3 FORMAT TSV"

echo "Constant column and count without columns:"
$CLICKHOUSE_LOCAL -q "SELECT 42 FROM file('$DATA_FILE', 'Vortex') LIMIT 2 FORMAT TSV"

echo "Reading a subcolumn of a nested structure with an explicit schema:"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, tuple(number, toString(number))::Tuple(a UInt64, b String) AS t
    FROM numbers(3)
    FORMAT Vortex" > "$DATA_FILE".nested
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE.nested', 'Vortex', '\`t.a\` UInt64') FORMAT TSV"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE.nested', 'Vortex', '\`t.b\` String, n UInt64') FORMAT TSV"
rm -f "$DATA_FILE".nested

echo "Empty file:"
$CLICKHOUSE_LOCAL -q "SELECT number AS n, toString(number) AS s FROM numbers(10) WHERE 0 FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', 'Vortex'); DESC file('$DATA_FILE', 'Vortex')"

echo "Larger data with compressible patterns survives the round trip:"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toString(number % 100) AS dict_like, randomString(10) AS rnd, number * 0 AS zeros
    FROM numbers(100000)
    FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(n), sum(cityHash64(dict_like) * 0 + 1), max(length(rnd)), sum(zeros)
    FROM file('$DATA_FILE', 'Vortex')"

echo "Magic bytes:"
head -c 4 "$DATA_FILE"
echo ""

echo "Truncated file produces an error:"
head -c 100 "$DATA_FILE" > "$DATA_FILE".truncated
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE.truncated', 'Vortex')" 2>&1 | grep -o -m1 "INCORRECT_DATA" || echo "no error"

rm -f "$DATA_FILE" "$DATA_FILE".truncated
