#!/usr/bin/env bash
# Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage, no-distributed-cache

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} "

DROP TABLE IF EXISTS test;
CREATE TABLE test (a String, b String, c String) ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS index_granularity = 11;

INSERT INTO test
SELECT round(pow(sipHash64(1, number), 1/4)), round(pow(sipHash64(2, number), 1/6)), round(pow(sipHash64(3, number), 1/10))
FROM numbers(100000);

INSERT INTO test
SELECT round(pow(sipHash64(1, number), 1/3)), round(pow(sipHash64(2, number), 1/5)), round(pow(sipHash64(3, number), 1/10))
FROM numbers(100000);

INSERT INTO test
SELECT round(pow(sipHash64(1, number), 1/5)), round(pow(sipHash64(2, number), 1/7)), round(pow(sipHash64(3, number), 1/10))
FROM numbers(100000);

DETACH TABLE test;
ATTACH TABLE test;
"

for i in {101..200}
do
    echo "
WITH ${i} AS try
SELECT try, count(), min(a), max(a), uniqExact(a), min(b), max(b), uniqExact(b), min(c), max(c), uniqExact(c) FROM test
WHERE a >= (round(pow(sipHash64(1, try), 1 / (3 + sipHash64(2, try) % 8))) AS a1)::String
  AND a <= (a1 + round(pow(sipHash64(3, try), 1 / (3 + sipHash64(4, try) % 8))))::String
  AND b >= (round(pow(sipHash64(5, try), 1 / (3 + sipHash64(6, try) % 8))) AS b1)::String
  AND b <= (b1 + round(pow(sipHash64(7, try), 1 / (3 + sipHash64(8, try) % 8))))::String
  AND c >= (round(pow(sipHash64(9, try), 1 / (3 + sipHash64(10, try) % 8))) AS c1)::String
  AND c <= (c1 + round(pow(sipHash64(11, try), 1 / (3 + sipHash64(12, try) % 8))))::String
HAVING count() > 0;
"
done | ${CLICKHOUSE_CLIENT}

${CLICKHOUSE_CLIENT} "DROP TABLE test"
