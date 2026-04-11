#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest

# Regression test for: https://github.com/ClickHouse/ClickHouse/issues/102153
#
# Reproduces the LOGICAL_ERROR in splitAndModifyMutationCommands via the NATURAL
# trigger path observed in CI: a part fetched from a more advanced replica has
# part.metadata_version > table.metadata_version when a mutation runs on r1.
#
# Natural trigger sequence (matches the CI crash at 2026-03-17 15:31:50):
#   1. Two replicas share a ZooKeeper path; r2's replication queue is frozen.
#   2. ADD COLUMN h is applied by r2 (schema v0 -> v1); r1's queue is frozen.
#   3. r2 inserts a part at schema v1 (metadata_version=1, contains column h).
#   4. The failpoint rmt_pause_before_apply_metadata_alter is enabled on this
#      server, then r1's queue is resumed.  r1's ALTER_METADATA thread enters
#      executeMetadataAlter and immediately blocks on the failpoint.
#   5. While r1 is stuck at schema v0, FETCH PART + ATTACH PART pulls the
#      advanced part (meta=1, has h) from r2 onto r1.
#      State: r1 has active part with metadata_version=1, table.metadata_version=0.
#   6. A regular mutation is submitted on r1.  Because it is not an alter-mutation
#      (alter_version == -1), its MUTATE_PART queue entry is NOT blocked by the
#      pending ALTER_METADATA.  A separate worker thread picks it up and calls
#      splitAndModifyMutationCommands.
#   7. Without the fix: part.metadata_version (1) > table.metadata_version (0)
#      and column h is absent from the table schema -> LOGICAL_ERROR thrown.
#      With the fix: the column is carried forward; mutation completes.
#   8. The failpoint is disabled so ALTER_METADATA can complete.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

DB="$CLICKHOUSE_DATABASE"
ZK_PATH="/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt_meta_race_natural"

# ---------------------------------------------------------------------------
# 1. Create two replicas.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS r1 SYNC;
    DROP TABLE IF EXISTS r2 SYNC;

    CREATE TABLE r1 (a UInt64, b UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt_meta_race_natural', 'r1')
    ORDER BY a
    SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;

    CREATE TABLE r2 (a UInt64, b UInt64 DEFAULT 0)
    ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt_meta_race_natural', 'r2')
    ORDER BY a
    SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
"

# ---------------------------------------------------------------------------
# 2. Insert initial data and sync.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "INSERT INTO r1 SELECT number, 0 FROM numbers(100)"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA r2"

# ---------------------------------------------------------------------------
# 3. Freeze r1's queue; let r2 apply ADD COLUMN h.
#    After this step: r2 is at schema v1 (has h); r1 is still at v0.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "SYSTEM STOP REPLICATION QUEUES r1"
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE r1 ADD COLUMN h String DEFAULT 'x'
    SETTINGS alter_sync=0
"

# Wait for r2 to have column h while r1 still does not.
for _ in {1..100}; do
    R2_HAS=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.columns
        WHERE database = '${DB}' AND table = 'r2' AND name = 'h'")
    R1_HAS=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.columns
        WHERE database = '${DB}' AND table = 'r1' AND name = 'h'")
    [ "$R2_HAS" = "1" ] && [ "$R1_HAS" = "0" ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    (SELECT count() FROM system.columns WHERE database = '${DB}' AND table = 'r2' AND name = 'h') != 1
    OR (SELECT count() FROM system.columns WHERE database = '${DB}' AND table = 'r1' AND name = 'h') != 0,
    'Schema precondition failed: expected h only in r2')" > /dev/null

# ---------------------------------------------------------------------------
# 4. r2 inserts a part with explicit h values (non-default) at schema v1.
#    SYSTEM SYNC REPLICA ensures r2 has committed this part.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "
    INSERT INTO r2 (a, b, h) SELECT number + 200, 0, concat('rep', toString(number)) FROM numbers(50)
"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA r2"

# Identify the advanced part on r2 that r1 does not have.
ADVANCED_PART=$($CLICKHOUSE_CLIENT -q "
    SELECT name FROM system.parts
    WHERE database = '${DB}' AND table = 'r2' AND active
      AND name NOT IN (
          SELECT name FROM system.parts
          WHERE database = '${DB}' AND table = 'r1' AND active
      )
    LIMIT 1
")

# ---------------------------------------------------------------------------
# 5. Enable the failpoint, then resume r1's queue.
#    r1's ALTER_METADATA thread will enter executeMetadataAlter and block
#    on the failpoint before applying the schema change.
#    r2 will NOT be affected: it already processed its ALTER_METADATA and
#    won't execute it again.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_pause_before_apply_metadata_alter"
$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES r1"

# Wait until the ALTER_METADATA thread has entered executeMetadataAlter and is
# blocked on the failpoint (is_currently_executing=1, column h still absent).
for _ in {1..100}; do
    EXECUTING=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.replication_queue
        WHERE database = '${DB}' AND table = 'r1' AND type = 'ALTER_METADATA'
          AND is_currently_executing = 1")
    R1_HAS=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.columns
        WHERE database = '${DB}' AND table = 'r1' AND name = 'h'")
    [ "$EXECUTING" = "1" ] && [ "$R1_HAS" = "0" ] && break
    sleep 0.1
done

# Hard assertion: r1 must still be at schema v0 with ALTER_METADATA paused.
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    (SELECT count() FROM system.replication_queue
     WHERE database = '${DB}' AND table = 'r1' AND type = 'ALTER_METADATA'
       AND is_currently_executing = 1) != 1,
    'ALTER_METADATA on r1 is not currently executing / paused by failpoint')" > /dev/null
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    (SELECT count() FROM system.columns WHERE database = '${DB}' AND table = 'r1' AND name = 'h') != 0,
    'r1 already applied ALTER_METADATA — failpoint did not pause it in time')" > /dev/null

# ---------------------------------------------------------------------------
# 6. FETCH the advanced part from r2 to r1, then ATTACH it.
#    After ATTACH: r1 has an active part with metadata_version=1 and column h,
#    but r1's own table.metadata_version is still 0 (ALTER_METADATA blocked).
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "ALTER TABLE r1 FETCH PART '${ADVANCED_PART}' FROM '${ZK_PATH}'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE r1 ATTACH PART '${ADVANCED_PART}'"

# Locate the newly activated part on r1.
PART_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT path FROM system.parts
    WHERE database = '${DB}' AND table = 'r1' AND active
    ORDER BY modification_time DESC
    LIMIT 1")
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    substring('${PART_PATH}', 1, 1) != '/',
    'Expected absolute path, got: ${PART_PATH}')" > /dev/null

PART_META=$(cat "${PART_PATH}metadata_version.txt")
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    ${PART_META} <= 0,
    'Expected fetched part metadata_version > 0, got ${PART_META}')" > /dev/null

# ---------------------------------------------------------------------------
# 7. Submit a regular mutation on r1.
#    With fix (Option C): the queue scheduler detects that source_part.metadata_version (1)
#    > table.metadata_version (0) and postpones the MUTATE_PART entry.  The mutation
#    remains pending while the failpoint holds ALTER_METADATA.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE r1 UPDATE b = 1 WHERE 1
    SETTINGS mutations_sync = 0
"

# Verify that the queue scheduler actually postponed the MUTATE_PART entry
# because source_part.metadata_version (1) > table.metadata_version (0).
# This directly tests the queue-level fix rather than just the end result.
for _ in {1..50}; do
    POSTPONED=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.replication_queue
        WHERE database = '${DB}' AND table = 'r1'
          AND type = 'MUTATE_PART'
          AND is_currently_executing = 0
          AND postpone_reason LIKE '%metadata_version%'")
    [ "$POSTPONED" = "1" ] && break
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "SELECT throwIf(
    (SELECT count() FROM system.replication_queue
     WHERE database = '${DB}' AND table = 'r1'
       AND type = 'MUTATE_PART'
       AND postpone_reason LIKE '%metadata_version%') = 0,
    'MUTATE_PART was not postponed by queue scheduler: queue-level fix did not trigger')" > /dev/null

# ---------------------------------------------------------------------------
# 8. Release the failpoint so ALTER_METADATA can complete.
#    Once table.metadata_version catches up to the part, the MUTATE_PART
#    postpone condition clears and the mutation executes normally.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_pause_before_apply_metadata_alter"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA r1"

# Poll up to ~30 s for the mutation to finish.
for _ in {1..150}; do
    DONE=$($CLICKHOUSE_CLIENT -q "
        SELECT min(is_done)
        FROM system.mutations
        WHERE database = '${DB}' AND table = 'r1'
          AND command LIKE '%UPDATE b = 1 WHERE 1%'")
    [ "$DONE" = "1" ] && break
    sleep 0.2
done

# ---------------------------------------------------------------------------
# 9. Report result.  With fix: is_done=1, had_logical_error=0.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -m -q "
    SELECT
        is_done,
        (latest_fail_reason LIKE '%LOGICAL_ERROR%') AS had_logical_error
    FROM system.mutations
    WHERE database = '${DB}' AND table = 'r1'
      AND command LIKE '%UPDATE b = 1 WHERE 1%'
    ORDER BY create_time
    LIMIT 1;
"

# ---------------------------------------------------------------------------
# 10. Verify h data in the attached part survived the mutation unchanged.
# ---------------------------------------------------------------------------
WRONG=$($CLICKHOUSE_CLIENT -q "
    SELECT count() FROM r1
    WHERE a >= 200 AND a < 250
      AND h != concat('rep', toString(a - 200))
")
$CLICKHOUSE_CLIENT -q "SELECT throwIf(${WRONG} != 0,
    'h data corrupted on r1 after mutation: found ${WRONG} rows with wrong values')" > /dev/null

DISTINCT_A=$($CLICKHOUSE_CLIENT -q "
    SELECT countDistinct(a) FROM r1
    WHERE a >= 200 AND a < 250
")
$CLICKHOUSE_CLIENT -q "SELECT throwIf(${DISTINCT_A} != 50,
    'Expected 50 rows in r1 for a in [200,250), got ${DISTINCT_A}')" > /dev/null

# ---------------------------------------------------------------------------
# Cleanup.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE r1 SYNC;
    DROP TABLE r2 SYNC;
"
