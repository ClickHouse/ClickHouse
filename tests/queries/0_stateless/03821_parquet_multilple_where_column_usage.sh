#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select * from file('$CUR_DIR/data_parquet/03821_file.parquet') prewhere x > 5 and (x > 6 or y > 3)"

$CLICKHOUSE_LOCAL -q "SELECT DISTINCT ((u16 < 4000) AND (u16 <= 61000)) OR assumeNotNull(2) OR ((u16 = 42) OR ((61000 >= u16) AND (4000 >= u16))), deltaSumDistinct(number) IGNORE NULLS FROM file('$CUR_DIR/data_parquet/03821_fil.parquet') PREWHERE and(and(notEquals(u32, (4000 >= u16) AND (u16 >= toUInt128(61000)) AS alias184), equals(dt64_ms, 4000 != u16)), equals(assumeNotNull(toNullable(56)), u16)) WHERE isNotDistinctFrom(u16, not(equals(u32, and(4000, (i8 >= -3) OR (assumeNotNull(42) = u16), 4000 != u16, equals(number, assumeNotNull(56))))))"
