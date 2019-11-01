#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS src_a;
DROP TABLE IF EXISTS src_b;

DROP TABLE IF EXISTS mv;

CREATE TABLE src_a(a UInt64) ENGINE = Null;
CREATE TABLE src_b(b UInt64) ENGINE = Null;

CREATE MATERIALIZED VIEW mv(v UInt64) Engine = MergeTree() ORDER BY v AS SELECT a as v FROM src_a;
EOF

# Useless test. Sometimes it catches a deadlock, other times it doesn't.
seq 1 100 | sed -r -e "s/.+/INSERT INTO TABLE src_a VALUES (1);/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &
seq 1 100 | sed -r -e "s/.+/INSERT INTO TABLE src_b VALUES (2);/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &
seq 1 100 | sed -r -e "s/.+/ALTER TABLE mv MODIFY QUERY SELECT a == 1 as v FROM src_a;/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &
seq 1 100 | sed -r -e "s/.+/ALTER TABLE mv MODIFY QUERY SELECT b == 2 as v FROM src_b;/" | $CLICKHOUSE_CLIENT -n 2>/dev/null &

wait

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM mv WHERE v == 1;"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv WHERE v == 0;"
