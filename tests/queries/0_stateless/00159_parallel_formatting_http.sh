#!/usr/bin/env bash
# Tags: stateful

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


FORMATS=('TSV' 'CSV' 'JSONCompactEachRow')

for format in "${FORMATS[@]}"
do
    echo "$format, false";
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+addSeconds('2025-01-01',number)+AS+a,number+AS+b,concat('value',number)+AS+c+FROM+(SELECT+number+FROM+system.numbers_mt+LIMIT+1000000)+ORDER+BY+number+Format+$format&output_format_parallel_formatting=false" -d' ' | md5sum

    echo "$format, true";
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+addSeconds('2025-01-01',number)+AS+a,number+AS+b,concat('value',number)+AS+c+FROM+(SELECT+number+FROM+system.numbers_mt+LIMIT+1000000)+ORDER+BY+number+Format+$format&output_format_parallel_formatting=true" -d' ' | md5sum
done
