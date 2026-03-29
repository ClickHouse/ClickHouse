#!/usr/bin/env bash
# Regression test for HashJoin::getNonJoinedBlocks assertion failure (STID 2980-385f).
# After join side swapping (e.g., LEFT ANTI → RIGHT ANTI), a column name can appear
# in both the left block and sample_block_with_columns_to_add. The assertion in
# getNonJoinedBlocks must account for this cross-side overlap.
# https://github.com/ClickHouse/ClickHouse/pull/100617

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_overlap"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_overlap (k UInt64) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_overlap VALUES (1), (2), (3)"

# Reproducer 1: GLOBAL NOT IN with join side swap creates column overlap.
# Must not crash with LOGICAL_ERROR. May return result or non-fatal error.
${CLICKHOUSE_CLIENT} --query="
SELECT
    (k GLOBAL NOT IN concat(*, modulo(-2 <=> (NULL IN (k)), multiply(toUInt32(*), (k IN (*)))))) > NULL,
    k GLOBAL NOT IN (SELECT DISTINCT 9223372036854775807)
FROM test_overlap
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
" 2>&1 | grep -v -c 'LOGICAL_ERROR' > /dev/null

${CLICKHOUSE_CLIENT} --query="DROP TABLE test_overlap"

# Reproducer 2: Decimal table with GLOBAL IN producing column overlap.
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_overlap2 (x Decimal(38, 2), y Nullable(Decimal(38, 2))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_overlap2 VALUES (32, 32), (64, 64), (128, 128), (256, 256)"

${CLICKHOUSE_CLIENT} --query="
SELECT *, '2149-06-06'
FROM test_overlap2
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
" 2>&1 | grep -v -c 'LOGICAL_ERROR' > /dev/null

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_overlap2"

# Reproducer 3: Simple join with duplicate column names.
# Verifies the fix preserves within-left duplicates correctly.
${CLICKHOUSE_CLIENT} --query="
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (a UInt64, b String) ENGINE = Memory;
CREATE TABLE t2 (a UInt64, c String) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 'x'), (2, 'y');
INSERT INTO t2 VALUES (1, 'p'), (3, 'q');
SELECT t1.a, t2.a, t1.b, t2.c FROM t1 FULL JOIN t2 ON t1.a = t2.a ORDER BY t1.a, t2.a;
DROP TABLE t1;
DROP TABLE t2;
"

echo "OK"
