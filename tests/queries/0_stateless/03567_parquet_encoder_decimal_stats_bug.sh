#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
$CLICKHOUSE_LOCAL -q "
    insert into function file('$file') select (number-1)::Decimal32(3)/123 as x from numbers(3);
    select 'all:', * from file('$file') order by x;
    select 'negative:', * from file('$file') where x < -0.007::Decimal32(3);
    select 'OH NO:', * from file('$file') where x < -0.008::Decimal32(3);
    select 'positive:', * from file('$file') where x > 0.007::Decimal32(3);
    select 'OH NO:', * from file('$file') where x > 0.008::Decimal32(3);"

rm "$file"
