#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An asynchronous insert flushes the data of several queries at once, which a non-parallel quorum
# insert - one in-flight quorum part per table - cannot honour, so such a combination is rejected
# with `UNSUPPORTED_PARAMETER`. The rejection used to happen only for an `INSERT` carrying its data
# inlined in the query text; an `INSERT` whose data is sent as blocks over the native protocol was
# queued anyway and failed in `ReplicatedMergeTreeSink` with a `LOGICAL_ERROR`.

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_05154 SYNC;
    CREATE TABLE t_05154 (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05154', 'r1') ORDER BY n;
"

quorum_settings="--async_insert=1 --wait_for_async_insert=1 --insert_quorum=2 --insert_quorum_parallel=0"

# Inlined data: the query text carries the values.
${CLICKHOUSE_CLIENT} $quorum_settings -q "INSERT INTO t_05154 VALUES (1)" 2>&1 | grep -om1 "UNSUPPORTED_PARAMETER"

# Data sent as blocks over the native protocol.
echo "2" | ${CLICKHOUSE_CLIENT} $quorum_settings -q "INSERT INTO t_05154 FORMAT TSV" 2>&1 | grep -om1 "UNSUPPORTED_PARAMETER"

# `insert_quorum = 'auto'` is a quorum too.
echo "3" | ${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --insert_quorum=auto --insert_quorum_parallel=0 \
    -q "INSERT INTO t_05154 FORMAT TSV" 2>&1 | grep -om1 "UNSUPPORTED_PARAMETER"

# A parallel quorum insert is accepted, and so is an asynchronous insert without a quorum.
echo "4" | ${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 --insert_quorum=1 --insert_quorum_parallel=0 \
    -q "INSERT INTO t_05154 FORMAT TSV"
echo "5" | ${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 -q "INSERT INTO t_05154 FORMAT TSV"

${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM t_05154;
    DROP TABLE t_05154 SYNC;
"
