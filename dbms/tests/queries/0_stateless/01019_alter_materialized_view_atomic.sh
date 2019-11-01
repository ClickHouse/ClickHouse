#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;

CREATE TABLE src(v UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv(v UInt8) Engine = MergeTree() ORDER BY v AS SELECT v FROM src;
EOF

ALTERS[0]="ALTER TABLE mv MODIFY QUERY SELECT v FROM src;"
ALTERS[1]="ALTER TABLE mv MODIFY QUERY SELECT v * 2 as v FROM src;"

for i in $(seq 1 100); do
  (
    # Retry (hopefully retriable (deadlock avoided)) errors.
    until false; do
      $CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1);" 2>/dev/null && break
    done
  ) &
done

for i in $(seq 1 100); do
  $CLICKHOUSE_CLIENT -q "${ALTERS[$RANDOM % 2]}" 2>/dev/null &
done

wait

$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv;"
