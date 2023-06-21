#!/usr/bin/env bash
# Tags: long, no-asan, no-msan, no-tsan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This query should return empty result in every of five runs:

for _ in {1..5}
do
    $CLICKHOUSE_CLIENT --compile_aggregate_expressions 0 --query "
SELECT
    COUNT() AS c,
    group_key,
    anyIf(r, key = 0) AS x0,
    anyIf(r, key = 1) AS x1,
    anyIf(r, key = 2) AS x2
FROM
(
    SELECT
        CRC32(toString(number)) % 1000000 AS group_key,
        number % 3 AS key,
        number AS r
    FROM numbers(10000000)
)
GROUP BY group_key
HAVING (c = 2) AND (x0 > 0) AND (x1 > 0) AND (x2 > 0)
ORDER BY group_key ASC
LIMIT 10
SETTINGS max_bytes_before_external_group_by = 200000
" && echo -n '.'
done

echo
