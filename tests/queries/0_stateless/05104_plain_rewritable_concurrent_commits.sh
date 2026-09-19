#!/usr/bin/env bash
# Tags: no-parallel, no-object-storage, no-replicated-database, no-shared-merge-tree, no-flaky-check
# Tag no-parallel: the pause failpoint is global to the server, so a concurrent test copying a file on any
#                  plain_rewritable disk would be parked by it, and `SYSTEM WAIT FAILPOINT` would synchronize
#                  with the wrong query
# Tag no-object-storage: the test uses a disk of its own
# Tag no-replicated-database: plain rewritable should not be shared between replicas
# Tag no-flaky-check: the failpoint is global to the server, a concurrent instance of this test would resume the paused query

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A metadata transaction on a plain_rewritable disk used to hold a disk-wide lock for the whole duration of its requests
# to the object storage, so a long operation on one table blocked every write to every other table on the same disk.
# Here MOVE PARTITION, which copies every file of the part, is paused in the middle of its commit,
# and an unrelated table on the same disk must go through its whole lifecycle in the meantime.
#
# The disk is used by this test only: a concurrent full reload of the in-memory metadata cache
# on the same disk would wait for the paused transaction, and every later transaction would wait behind the reload.

DISK="local_plain_rewritable_05104"
FAILPOINT="plain_object_storage_pause_on_file_copy"
MOVE_QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_move_partition"
# Packed part storage is randomized by the test runner; pin full part storage so that the part is always moved
# by the file copy operation the failpoint is attached to.
PART_STORAGE="min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, min_level_for_full_part_storage = 0"

# Never leave the server-wide failpoint enabled, or every later test copying a file on a plain_rewritable disk hangs.
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}" >/dev/null 2>&1
    if [[ -n "${move_pid:-}" ]]; then
        wait "${move_pid}" >/dev/null 2>&1
    fi
    ${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS t_src SYNC;
    DROP TABLE IF EXISTS t_dst SYNC;
    DROP TABLE IF EXISTS t_other SYNC" >/dev/null 2>&1
}
trap cleanup EXIT
# Make a termination signal run the `EXIT` trap instead of killing the shell outright.
trap 'exit 143' INT TERM

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t_src SYNC;
DROP TABLE IF EXISTS t_dst SYNC;
DROP TABLE IF EXISTS t_other SYNC;

CREATE TABLE t_src (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}', ${PART_STORAGE};
CREATE TABLE t_dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}', ${PART_STORAGE};
INSERT INTO t_src SELECT number, toString(number) FROM numbers(1000);

SYSTEM ENABLE FAILPOINT ${FAILPOINT};
"

${CLICKHOUSE_CLIENT} --query_id "${MOVE_QUERY_ID}" --query "ALTER TABLE t_src MOVE PARTITION tuple() TO TABLE t_dst" &
move_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE t_other (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}', ${PART_STORAGE};
INSERT INTO t_other SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_other SELECT number, toString(number) FROM numbers(100, 100);
OPTIMIZE TABLE t_other FINAL;
SELECT 'other table', count(), sum(a), uniqExact(_part) FROM t_other;
TRUNCATE TABLE t_other;
SELECT 'other table truncated', count() FROM t_other;
DROP TABLE t_other SYNC;

SELECT 'move is still running', count() FROM system.processes WHERE query_id = '${MOVE_QUERY_ID}';
"

# In its own invocation: a failing statement in the batch above aborts the rest of it, and the paused
# MOVE PARTITION would then never be resumed, turning a failed assertion into a test timeout.
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"

wait ${move_pid}
unset move_pid

${CLICKHOUSE_CLIENT} -m --query "
SELECT 'source', count() FROM t_src;
SELECT 'destination', count(), sum(a) FROM t_dst;

DROP TABLE t_src SYNC;
DROP TABLE t_dst SYNC;
"
