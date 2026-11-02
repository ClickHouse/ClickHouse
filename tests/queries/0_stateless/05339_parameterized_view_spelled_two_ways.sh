#!/usr/bin/env bash
# Tags: shard

# A parameterized view called as `pv` and as `<database>.pv` in one query is one table expression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (2);
    CREATE VIEW pv AS SELECT x FROM src WHERE x >= {p:UInt64};
    CREATE TABLE data (number UInt64) ENGINE = MergeTree ORDER BY number;
    INSERT INTO data SELECT number FROM numbers(4);
    CREATE TABLE dist AS data ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data);
"

$CLICKHOUSE_CLIENT -q "
    SELECT number IN (SELECT x FROM pv(p = 0)) AS k, count() FROM data
    GROUP BY number IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.pv(p = 0)) ORDER BY k"

$CLICKHOUSE_CLIENT -q "
    SELECT number IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.pv(p = 0)) AS k, count() FROM dist
    GROUP BY number IN (SELECT x FROM pv(p = 0)) ORDER BY k"

# Different arguments, modifiers or settings are still different table expressions.
for rhs in "pv(p = 3)" "pv(p = 0) FINAL" "pv(p = 0, SETTINGS max_threads = 1)"; do
    $CLICKHOUSE_CLIENT -q "
        SELECT number IN (SELECT x FROM pv(p = 0)) AS k, count() FROM data
        GROUP BY number IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.$rhs)" 2>&1 | grep -o -m1 'NOT_AN_AGGREGATE'
done
