#!/usr/bin/env bash

set -e

if ! hash dot >/dev/null 2>&1 ; then
    echo "@@SKIP@@: no dot"
    exit 0
fi

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    --max_threads=4
)

# Setup
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t1;
CREATE TABLE t1
(
    a1 UInt64,
    a2 UInt64,
    a3 UInt64,
    a4 UInt64
)
ENGINE = MergeTree
ORDER BY (a1, a2, a3);

DROP TABLE IF EXISTS t2;
CREATE TABLE t2
(
    b1 UInt64,
    b2 UInt64,
    b3 UInt64,
    b4 UInt64
)
ENGINE = MergeTree
ORDER BY (b1, b2, b3);

INSERT INTO t1 SELECT floor(randNormal(100, 5)), floor(randNormal(10, 1)), floor(randNormal(100, 2)), floor(randNormal(100, 10)) FROM numbers(10);

INSERT INTO t2 SELECT floor(randNormal(100, 1)), floor(randNormal(10, 1)), floor(randNormal(100, 10)), floor(randNormal(100, 2)) FROM numbers(10000);
"

query="
SELECT count(*)
FROM
(
    SELECT
        t1.a1,
        t2.b3,
        t2.b4
    FROM t2
    INNER JOIN t1 ON (t1.a2 = t2.b2) AND (t1.a1 = t2.b1)
);
"

# Test Case 1: The graphviz is well formed without analyzer
$CLICKHOUSE_CLIENT --enable_analyzer=0 "${opts[@]}" -q "EXPLAIN ANALYZE $query" | dot -Tpng > /dev/null
echo $?


# Test Case 2: The graphviz is well formed without analyzer with analyzer
$CLICKHOUSE_CLIENT --enable_analyzer=1 "${opts[@]}" -q "EXPLAIN ANALYZE $query" | dot -Tpng > /dev/null
echo $?
