#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
# A mixed DROP COLUMN + MODIFY COMMENT shrinks the final metadata below max_query_size,
# but the intermediate comment-only metadata (current columns + long comment) stays over it.
# The rejected ALTER must not leave the comment applied in memory.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS r SYNC"

bigcol="big_$(printf 'x%.0s' {1..400})"
cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int8', range(20)), ', ')")
$CLICKHOUSE_CLIENT -q "CREATE TABLE r (${cols}, ${bigcol} Int8) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/r', '1') ORDER BY c0"

comment=$(printf 'C%.0s' $(seq 1 150))
$CLICKHOUSE_CLIENT --max_query_size=1024 -q "ALTER TABLE r DROP COLUMN ${bigcol}, MODIFY COMMENT '${comment}'" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# The rejected ALTER must not have applied the comment in memory.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'r' AND comment != ''"

$CLICKHOUSE_CLIENT -q "DROP TABLE r SYNC"
