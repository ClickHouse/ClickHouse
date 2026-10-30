#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-parallel

# `SYSTEM RESTORE REPLICA` reattaches every part under a freshly allocated block number, so any
# name queued for check beforehand stops denoting anything. Left queued, such a name is read as a
# lost part and `createEmptyPartInsteadOfLost` recreates it over a block range that now holds
# unrelated data; the rejected write leaves a zero-row part behind that breaks the next attach.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t SELECT number FROM numbers(5);
    INSERT INTO t SELECT number FROM numbers(5, 5);
    OPTIMIZE TABLE t FINAL;
"

# A part spanning several blocks, so a stale name would cover the blocks reissued after the restore.
${CLICKHOUSE_CLIENT} -q "SELECT 'parts before', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"

${CLICKHOUSE_CLIENT} -q "
    DETACH TABLE t;
    ATTACH TABLE t AS REPLICATED;
"

# The part is on disk but absent from Keeper, so the check is deferred rather than resolved.
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT 'queued before restore', parts_to_check > 0 FROM system.replicas WHERE database = currentDatabase() AND table = 't'"

${CLICKHOUSE_CLIENT} -q "SYSTEM RESTORE REPLICA t"
${CLICKHOUSE_CLIENT} -q "SELECT 'queued after restore', parts_to_check FROM system.replicas WHERE database = currentDatabase() AND table = 't'"

${CLICKHOUSE_CLIENT} -q "SELECT 'rows', count() FROM t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"
