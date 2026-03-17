#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
CREATE TABLE t (a UInt8, b UInt8) ORDER BY (a, b);
INSERT INTO t VALUES (0,0);
"

$CLICKHOUSE_CLIENT -n -q "EXPLAIN indexes = 1 SELECT * FROM t WHERE a = 0;" | grep "Search Algorithm: binary search"
$CLICKHOUSE_CLIENT -n -q "EXPLAIN indexes = 1 SELECT * FROM t WHERE b = 0;" | grep "Search Algorithm: generic exclusion search"
