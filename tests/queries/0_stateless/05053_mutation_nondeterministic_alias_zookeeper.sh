#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# no-shared-merge-tree: non deterministic is just allowed with shared merge tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_NAME=mutation_nondeterministic_alias_05053

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $TABLE_NAME SYNC"

${CLICKHOUSE_CLIENT} --query "
	CREATE TABLE $TABLE_NAME
	(
		p UInt32,
		x UInt64,
		r UInt32 ALIAS toUnixTimestamp(now()),
		e UInt32 EPHEMERAL toUnixTimestamp(now())
	)
	ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE_NAME', 'r1')
	PARTITION BY p
	ORDER BY x"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE_NAME DELETE WHERE p < r" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE_NAME DELETE WHERE p < e" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "DROP TABLE $TABLE_NAME SYNC"
