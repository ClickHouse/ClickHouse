#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-flaky-check
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
# This test does not run such a reload itself, so it needs no `no-parallel` tag.

DISK="local_plain_rewritable_05104"
FAILPOINT="plain_object_storage_pause_on_file_copy"
MOVE_QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_move_partition"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t_src SYNC;
DROP TABLE IF EXISTS t_dst SYNC;
DROP TABLE IF EXISTS t_other SYNC;

CREATE TABLE t_src (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}';
CREATE TABLE t_dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}';
INSERT INTO t_src SELECT number, toString(number) FROM numbers(1000);

SYSTEM ENABLE FAILPOINT ${FAILPOINT};
"

${CLICKHOUSE_CLIENT} --query_id "${MOVE_QUERY_ID}" --query "ALTER TABLE t_src MOVE PARTITION tuple() TO TABLE t_dst" &
move_pid=$!

${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE t_other (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}';
INSERT INTO t_other SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_other SELECT number, toString(number) FROM numbers(100, 100);
OPTIMIZE TABLE t_other FINAL;
SELECT 'other table', count(), sum(a), uniqExact(_part) FROM t_other;
TRUNCATE TABLE t_other;
SELECT 'other table truncated', count() FROM t_other;
DROP TABLE t_other SYNC;

SELECT 'move is still running', count() FROM system.processes WHERE query_id = '${MOVE_QUERY_ID}';

SYSTEM DISABLE FAILPOINT ${FAILPOINT};
"

wait ${move_pid}

${CLICKHOUSE_CLIENT} -m --query "
SELECT 'source', count() FROM t_src;
SELECT 'destination', count(), sum(a) FROM t_dst;

DROP TABLE t_src SYNC;
DROP TABLE t_dst SYNC;
"
