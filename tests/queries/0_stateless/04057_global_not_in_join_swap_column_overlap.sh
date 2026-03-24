#!/usr/bin/env bash
# Regression test: GLOBAL NOT IN with join side swap caused
# "Unexpected number of columns in result sample block" exception in HashJoin::getNonJoinedBlocks
# when a column name appeared in both left and right sides after the swap.
# https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=100378&sha=ce356689f6a6d126e078f5cbfcbab8b3849de673&name_0=PR&name_1=AST%20fuzzer%20%28amd_tsan%29

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_table"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_table (k UInt64) ENGINE = MergeTree ORDER BY k"

# The query should not cause a LOGICAL_ERROR (server crash in debug builds).
# It may return an error or empty result — either is acceptable.
${CLICKHOUSE_CLIENT} --query="
SELECT
    (k GLOBAL NOT IN concat(*, modulo(-2 <=> (NULL IN (k)), multiply(toUInt32(*), (k IN (*)))))) > NULL,
    k GLOBAL NOT IN (SELECT DISTINCT 9223372036854775807)
FROM test_table
PREWHERE toLowCardinality(toNullable(9223372036854775807))
WHERE ((NULL >= (multiply((* GLOBAL IN (k)), 257) NOT IN (k)))
    AND (multiply(*, minus((SELECT toUInt32(9223372036854775806)
        GROUP BY * = (k IN (7)),
        concat(modulo(7 >= (255 GLOBAL NOT IN (k)), minus((* IN (k)), toUInt32(-2147483649))), *)),
        (k NOT IN (*)))) GLOBAL NOT IN (k))
    AND ((SELECT intDiv(NULL, (divide(k, materialize(0)) GLOBAL NOT IN (-2))))
        <=> (SELECT toNullable(-9223372036854775807) GLOBAL NOT IN modulo(65536, k)))
    AND ((SELECT DISTINCT 2) = (* GLOBAL NOT IN (k))))
    <= toInt64(materialize(9223372036854775807))
FORMAT Null
SETTINGS enable_analyzer = 1
" >/dev/null 2>&1 ||:

${CLICKHOUSE_CLIENT} --query="DROP TABLE test_table"

# Second reproducer from https://github.com/ClickHouse/ClickHouse/issues/100422
${CLICKHOUSE_CLIENT} --query="CREATE TABLE temp (x Decimal(38, 2), y Nullable(Decimal(38, 2))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO temp VALUES (32, 32), (64, 64), (128, 128), (256, 256)"

${CLICKHOUSE_CLIENT} --query="
SELECT *, '2149-06-062149-06-062149-06-062149-06-062149-06-062149-06-062149-06-062149-06-06'
FROM temp
WHERE toUInt8(multiIf(
    toInt32(256, concat((SELECT *, plus(NULL, 2147483647))), NULL),
    toInt32(moduloOrZero(-2147483647, (SELECT if(
        and(globalIn(y, tuple(intDiv(-9223372036854775808, toInt32(moduloOrZero(toInt256(toLowCardinality(1023)),
            (SELECT toLowCardinality(-9223372036854775808)))))
        , globalIn(toNullable(257), 65537))),
        equals(minus(toUInt32(toNullable(9223372036854775807),
            intDivOrZero(NULL, toInt8(toLowCardinality(toNullable(1024))))), NULL),
            y AS alias3719)),
        -9223372036854775808,
        concat(toUInt16(256)))))),
    concat(multiply(NULL, toInt128(1024)), toInt16((SELECT NULL)),
        toUInt32(9223372036854775807, NULL, toInt128(1024), 7), '', 257)))
    IN (1025, toInt128(materialize(9223372036854775806)))
FORMAT Null
SETTINGS enable_analyzer = 1
" >/dev/null 2>&1 ||:

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS temp"

echo "OK"
