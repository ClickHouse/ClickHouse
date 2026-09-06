#!/usr/bin/env bash
# Tags: no-parallel, no-async-insert
# Tag no-parallel: Fails due to failpoint intersection

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

set -e

# Both failpoints below are server-wide, so an early exit under `set -e` would leave every later
# mutation on this server seeing a 1 byte budget or no free threads. Best effort, on top of the
# explicit DISABLE statements rather than instead of them.
trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
" 2>/dev/null || true' EXIT

# mt_select_parts_to_mutate_max_part_size forces the free-space budget of every mutation to 1 byte, so
# every part exceeds it and no real disk pressure is needed. A mutation that only hardlinks the files it
# does not touch must be admitted anyway; anything that rewrites the part must still be postponed.

# Wait until a non-done mutation of the table has a non-empty parts_postpone_reasons. Safe to poll: the
# failpoint keeps the state stable once set.
function wait_for_postpone_reasons()
{
    local table=$1
    for _ in $(seq 1 300); do
        result=$($CLICKHOUSE_CLIENT --query "
            SELECT count()
            FROM system.mutations
            WHERE database = '$CLICKHOUSE_DATABASE' AND table = '$table'
              AND NOT is_done AND notEmpty(parts_postpone_reasons)
        ")
        if [ "$result" -gt 0 ]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for parts_postpone_reasons of $table" >&2
    return 1
}

# Report whether the single non-done mutation of the table is postponed for exceeding the part size, and
# that it did not fail (a postpone must never become a failure).
function report_postponed()
{
    local table=$1
    wait_for_postpone_reasons "$table"
    $CLICKHOUSE_CLIENT --query "
        SELECT
            '$table',
            'postponed',
            arrayExists(reason -> reason = 'Exceed max source part size', mapValues(parts_postpone_reasons)),
            'no_failure',
            empty(latest_fail_reason)
        FROM system.mutations
        WHERE database = '$CLICKHOUSE_DATABASE' AND table = '$table' AND NOT is_done
    "
}

##########################################################################################
# The reported scenario: DROP INDEX on a part larger than the budget. The mutation unlinks the index
# files and hardlinks everything else, so it must complete. Default statistics configuration on purpose:
# auto_statistics_types is non-empty by default, so a merged part carries statistics.packed, and the fix
# has to hardlink that too rather than refusing the part.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_index_oversized (event String, id UInt64,
        INDEX idx_event event TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0;

    INSERT INTO drop_index_oversized SELECT repeat('abcdefgh', 20), number FROM numbers(20000);
    OPTIMIZE TABLE drop_index_oversized FINAL;
"

# The part must be the shape the fix targets, and it must carry statistics that the mutation has to keep,
# so that the case is positive proof that a default-settings part is admitted rather than refused for
# having them. packed_skip_index_max_bytes = 0 keeps the packed skip-index archive - the one output of the
# partial route that really does copy data, and hence the one shape the fix deliberately excludes - out of
# the part, so the case cannot silently exercise the excluded path.
$CLICKHOUSE_CLIENT --query "
    SELECT 'fixture', part_type, part_storage_type
    FROM system.parts
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND active;
"

# How many columns carry statistics depends on auto_statistics_types, which the test runner randomizes,
# so record the number rather than asserting one - what matters is that there ARE some and that the
# mutation keeps exactly them.
statistics_before=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND active
      AND notEmpty(statistics)
")
echo "fixture	has_statistics	$([ "$statistics_before" -gt 0 ] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE drop_index_oversized DROP INDEX idx_event SETTINGS alter_sync = 0;
"

wait_for_mutation "drop_index_oversized" "mutation_2.txt"

# Liveness: the index is really gone and the data is still there, so the case cannot pass by doing
# nothing. Plus the part is intact: the statistics archive was hardlinked with its inherited checksums,
# and a mismatch between checksums.txt and the files would show up here.
$CLICKHOUSE_CLIENT --query "
    SELECT 'reported_case', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND NOT is_done;

    SELECT 'reported_case', 'failed', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND notEmpty(latest_fail_reason);

    SELECT 'reported_case', 'indices_left', count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'drop_index_oversized';

    SELECT 'reported_case', 'rows', count() FROM drop_index_oversized;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE drop_index_oversized" | while read -r line; do
    echo "reported_case	check	$line"
done

# The mutation must have taken the partial route - hardlink what it does not touch - rather than
# succeeding by accidentally rewriting the whole part, which needs the space the part was postponed for.
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS part_log;
    SELECT 'reported_case', 'route_partial', sum(ProfileEvents['MutationSomePartColumns']) > 0,
        'route_full', sum(ProfileEvents['MutationAllPartColumns'])
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND event_type = 'MutatePart';
"

# The statistics survive the mutation: the archive holding them is unchanged by a DROP INDEX, so it must
# come across with everything else the mutation does not touch. Compared against the fixture's own count
# so the assertion holds whichever statistics the runner's randomization produced.
statistics_after=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'drop_index_oversized' AND active
      AND notEmpty(statistics)
")
echo "reported_case	statistics_kept	$([ "$statistics_after" = "$statistics_before" ] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    DROP TABLE drop_index_oversized SYNC;
"

##########################################################################################
# DROP PROJECTION is the other command in the exempt class, so it needs its own positive case: it
# unlinks a directory rather than a few files, and the changelog promises it.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_projection_oversized (id UInt64, s String,
        PROJECTION proj_s (SELECT s, count() GROUP BY s))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0, deduplicate_merge_projection_mode = 'rebuild';
    INSERT INTO drop_projection_oversized SELECT number, repeat('abcdefgh', 20) FROM numbers(20000);
    OPTIMIZE TABLE drop_projection_oversized FINAL;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE drop_projection_oversized DROP PROJECTION proj_s SETTINGS alter_sync = 0;
"

wait_for_mutation "drop_projection_oversized" "mutation_2.txt"

$CLICKHOUSE_CLIENT --query "
    SELECT 'drop_projection', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'drop_projection_oversized' AND NOT is_done;

    SELECT 'drop_projection', 'projections_left', count() FROM system.projections
    WHERE database = currentDatabase() AND table = 'drop_projection_oversized';

    SELECT 'drop_projection', 'rows', count() FROM drop_projection_oversized;

    SYSTEM FLUSH LOGS part_log;
    SELECT 'drop_projection', 'route_partial', sum(ProfileEvents['MutationSomePartColumns']) > 0,
        'route_full', sum(ProfileEvents['MutationAllPartColumns'])
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 'drop_projection_oversized' AND event_type = 'MutatePart';
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE drop_projection_oversized" | while read -r line; do
    echo "drop_projection	check	$line"
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    DROP TABLE drop_projection_oversized SYNC;
"

##########################################################################################
# Negative controls. Each pins one condition the classifier refuses, and each must STILL be postponed
# with the same reason while the failpoint is on: without them, widening the exempt class is invisible.
##########################################################################################

# A DELETE rewrites the part.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE mutation_delete (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0;
    INSERT INTO mutation_delete SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE mutation_delete FINAL;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE mutation_delete DELETE WHERE id = 1 SETTINGS alter_sync = 0;
"
report_postponed "mutation_delete"

# A Compact part is rewritten in full whatever the command is.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_index_compact (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
             packed_skip_index_max_bytes = 0;
    INSERT INTO drop_index_compact SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE drop_index_compact FINAL;
    ALTER TABLE drop_index_compact DROP INDEX idx_id SETTINGS alter_sync = 0;
"
$CLICKHOUSE_CLIENT --query "
    SELECT 'drop_index_compact', 'part_type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'drop_index_compact' AND active
"
report_postponed "drop_index_compact"

# So is a Wide part in packed storage.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_index_packed (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000,
             min_rows_for_full_part_storage = 1000000000, packed_skip_index_max_bytes = 0;
    INSERT INTO drop_index_packed SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE drop_index_packed FINAL;
    ALTER TABLE drop_index_packed DROP INDEX idx_id SETTINGS alter_sync = 0;
"
$CLICKHOUSE_CLIENT --query "
    SELECT 'drop_index_packed', 'part_storage_type', part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'drop_index_packed' AND active
"
report_postponed "drop_index_packed"

# Copying instead of hardlinking rewrites every retained file.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE drop_index_copying (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0, always_use_copy_instead_of_hardlinks = 1;
    INSERT INTO drop_index_copying SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE drop_index_copying FINAL;
    ALTER TABLE drop_index_copying DROP INDEX idx_id SETTINGS alter_sync = 0;
"
report_postponed "drop_index_copying"

# CLEAR INDEX is deliberately outside the exempt class: it rebuilds what it clears elsewhere.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE clear_index_oversized (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0;
    INSERT INTO clear_index_oversized SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE clear_index_oversized FINAL;
    ALTER TABLE clear_index_oversized CLEAR INDEX idx_id SETTINGS alter_sync = 0;
"
report_postponed "clear_index_oversized"

# DROP STATISTICS is interpreter-free like DROP INDEX, but it changes which statistics the part holds, so
# the archive has to be written anew and its size is not covered by the small reservation. Excluded.
$CLICKHOUSE_CLIENT --query "
    SET allow_statistics = 1;
    CREATE TABLE drop_statistics_oversized (id UInt64, v UInt64 STATISTICS(tdigest))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO drop_statistics_oversized SELECT number, number % 100 FROM numbers(20000);
    OPTIMIZE TABLE drop_statistics_oversized FINAL;
    ALTER TABLE drop_statistics_oversized DROP STATISTICS v SETTINGS alter_sync = 0;
"
report_postponed "drop_statistics_oversized"

# The exemption must stay inside the size guard, below the throttle: with no thread available for
# mutations the exempt DROP INDEX must be postponed too, for the throttle's own reason.
$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    CREATE TABLE drop_index_throttled (id UInt64, s String, INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0;
    INSERT INTO drop_index_throttled SELECT number, repeat('x', 100) FROM numbers(20000);
    OPTIMIZE TABLE drop_index_throttled FINAL;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE drop_index_throttled DROP INDEX idx_id SETTINGS alter_sync = 0;
"
wait_for_postpone_reasons "drop_index_throttled"
$CLICKHOUSE_CLIENT --query "
    SELECT 'drop_index_throttled', 'throttled',
        arrayExists(reason -> reason = 'No free threads in pool', mapValues(parts_postpone_reasons)),
        'no_failure', empty(latest_fail_reason)
    FROM system.mutations
    WHERE database = currentDatabase() AND table = 'drop_index_throttled' AND NOT is_done
"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;"
wait_for_mutation "drop_index_throttled" "mutation_2.txt"

##########################################################################################
# Statistics handling, with the budget back to normal. These are about what the exempt route WRITES, not
# about admission, so they run without the failpoint.
##########################################################################################

# An exempt DROP INDEX leaves the statistics alone, so the archive keeps its inherited checksums. A
# DROP STATISTICS on the same shape does change them and must still write a new archive.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE keep_statistics (id UInt64, v UInt64 STATISTICS(tdigest),
        INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0;
    INSERT INTO keep_statistics SELECT number, number % 100 FROM numbers(20000);
    OPTIMIZE TABLE keep_statistics FINAL;
    ALTER TABLE keep_statistics DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;

    SELECT 'keep_statistics', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'keep_statistics' AND NOT is_done;
    SELECT 'keep_statistics', 'indices_left', count() FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 'keep_statistics';
    SELECT 'keep_statistics', 'statistics_readable', count() FROM keep_statistics WHERE v < 50;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE keep_statistics" | while read -r line; do
    echo "keep_statistics	check	$line"
done

$CLICKHOUSE_CLIENT --query "
    ALTER TABLE keep_statistics DROP STATISTICS v SETTINGS alter_sync = 2, mutations_sync = 2;
    SELECT 'rewrite_statistics', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'keep_statistics' AND NOT is_done;
    SELECT 'rewrite_statistics', 'statistics_left', count() FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 'keep_statistics' AND active AND notEmpty(statistics);
    SELECT 'rewrite_statistics', 'rows', count() FROM keep_statistics;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE keep_statistics" | while read -r line; do
    echo "rewrite_statistics	check	$line"
done

##########################################################################################
# What the exempt route WRITES, measured rather than asserted. Two tables of the same shape, both with
# wide statistics: one gets a DROP INDEX (which cannot change the statistics, so the archive is
# hardlinked) and one a DROP STATISTICS (which does change them, so it is written anew). Comparing the
# two mutations' write footprints needs no threshold and no constant, and it is what notices a
# regression that re-serializes an identical archive - something an "it completed" assertion cannot see.
#
# The byte counter below is WriteBufferFromFileDescriptorWriteBytes, which counts local file
# descriptor writes only, so every table whose footprint it measures pins storage_policy = 'default'.
# On an object storage policy the part data goes to the remote buffer instead and the counter is left
# measuring local metadata, which is the same size on both sides of each comparison.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    SET allow_statistics = 1;
    CREATE TABLE write_footprint_keep (id UInt64,
        a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64 STATISTICS(tdigest),
        d UInt64 STATISTICS(tdigest), e UInt64 STATISTICS(tdigest), f UInt64 STATISTICS(tdigest),
        INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0,
             storage_policy = 'default';

    CREATE TABLE write_footprint_rewrite AS write_footprint_keep;

    /* Same shape, except that the skip indices live in a packed archive. Rebuilding that archive really
       does copy data, so such a mutation is NOT in the exempt class for ADMISSION - while still taking the
       partial route, and while its commands still cannot change which statistics the part holds. It is
       therefore the case that shows the statistics decision is made from the commands rather than from the
       admission flag: this table preserves although it was never admitted as hardlink-only. */
    CREATE TABLE write_footprint_nonexempt (id UInt64,
        a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest), c UInt64 STATISTICS(tdigest),
        d UInt64 STATISTICS(tdigest), e UInt64 STATISTICS(tdigest), f UInt64 STATISTICS(tdigest),
        INDEX idx_id id TYPE minmax GRANULARITY 1, INDEX idx_a a TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 1048576, storage_policy = 'default';

    /* Its statistics-free twin. Whatever the two write in common cancels out, so the difference between
       them isolates the statistics write that WOULD occur on regression: near nothing while the decision
       is derived from the commands, and the whole archive again if it went back to keying on admission. */
    CREATE TABLE write_footprint_nonexempt_baseline (id UInt64,
        a UInt64, b UInt64, c UInt64, d UInt64, e UInt64, f UInt64,
        INDEX idx_id id TYPE minmax GRANULARITY 1, INDEX idx_a a TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 1048576, auto_statistics_types = '',
             storage_policy = 'default';

    INSERT INTO write_footprint_keep SELECT number,
        number * 7, number * 11, number * 13, number * 17, number * 19, number * 23 FROM numbers(50000);
    INSERT INTO write_footprint_rewrite SELECT number,
        number * 7, number * 11, number * 13, number * 17, number * 19, number * 23 FROM numbers(50000);
    INSERT INTO write_footprint_nonexempt SELECT number,
        number * 7, number * 11, number * 13, number * 17, number * 19, number * 23 FROM numbers(50000);
    INSERT INTO write_footprint_nonexempt_baseline SELECT number,
        number * 7, number * 11, number * 13, number * 17, number * 19, number * 23 FROM numbers(50000);
    OPTIMIZE TABLE write_footprint_keep FINAL;
    OPTIMIZE TABLE write_footprint_rewrite FINAL;
    OPTIMIZE TABLE write_footprint_nonexempt FINAL;
    OPTIMIZE TABLE write_footprint_nonexempt_baseline FINAL;

    ALTER TABLE write_footprint_keep DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE write_footprint_rewrite DROP STATISTICS a SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE write_footprint_nonexempt DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE write_footprint_nonexempt_baseline DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;

    SYSTEM FLUSH LOGS part_log;

    SELECT 'write_footprint', 'keep_writes_less_than_rewrite',
        sumIf(bytes, table = 'write_footprint_keep') < sumIf(bytes, table = 'write_footprint_rewrite'),
        /* Both sides are one statistics write, isolated by a same-shape twin, so the comparison needs no
           threshold and no constant. Left: what the non-exempt partial-route mutation writes over its
           statistics-free twin - it must be near nothing, because its commands cannot change which
           statistics the part holds even though it was never admitted as hardlink-only. Right: what a real
           change to the statistics set costs, measured on the identical fixture. The assertion is that the
           first is now the smaller of the two, and it goes red both if the non-exempt route starts writing
           the archive again and if a real change stops writing it. */
        'non_exempt_preserves_statistics_too',
        toInt64(sumIf(bytes, table = 'write_footprint_nonexempt'))
                - toInt64(sumIf(bytes, table = 'write_footprint_nonexempt_baseline'))
            < toInt64(sumIf(bytes, table = 'write_footprint_rewrite'))
                - toInt64(sumIf(bytes, table = 'write_footprint_keep'))
    FROM (
        SELECT table, ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS bytes
        FROM system.part_log
        WHERE database = currentDatabase() AND event_type = 'MutatePart'
          AND table IN ('write_footprint_keep', 'write_footprint_rewrite',
                        'write_footprint_nonexempt', 'write_footprint_nonexempt_baseline')
    );
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE write_footprint_keep" | while read -r line; do
    echo "write_footprint	keep_check	$line"
done
$CLICKHOUSE_CLIENT --query "CHECK TABLE write_footprint_rewrite" | while read -r line; do
    echo "write_footprint	rewrite_check	$line"
done
$CLICKHOUSE_CLIENT --query "CHECK TABLE write_footprint_nonexempt" | while read -r line; do
    echo "write_footprint	non_exempt_check	$line"
done

##########################################################################################
# A mutation that REWRITES a column's values must write its statistics anew even when its commands
# cannot change WHICH statistics the part holds. MATERIALIZE COLUMN on a MATERIALIZED column is the
# case: it reaches the interpreter and rewrites the values, while being in none of the command types
# the preservation decision tests, so a decision made from those alone would hardlink an archive that
# no longer describes the data.
#
# Measured the same threshold-free way as write_footprint above, with two independent differences:
#   mat_st - mat_nost   what MATERIALIZE COLUMN writes over its statistics-free twin: the archive
#   drop_st - drop_nost what DROP INDEX writes over its twin: near nothing, since it does preserve
# The assertion is that the first exceeds the second, so it goes red both if the rewriting mutation
# starts preserving and if the preserving one stops.
##########################################################################################

$CLICKHOUSE_CLIENT --query "
    SET allow_statistics = 1;
    CREATE TABLE materialize_column_st (id UInt64, base UInt64,
        a UInt64 MATERIALIZED base * 1 STATISTICS(tdigest), b UInt64 MATERIALIZED base * 2 STATISTICS(tdigest),
        c UInt64 MATERIALIZED base * 3 STATISTICS(tdigest), d UInt64 MATERIALIZED base * 4 STATISTICS(tdigest),
        e UInt64 MATERIALIZED base * 5 STATISTICS(tdigest), f UInt64 MATERIALIZED base * 6 STATISTICS(tdigest),
        INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0, auto_statistics_types = '',
             storage_policy = 'default';

    /* Statistics-free twin: whatever the two write in common cancels out. */
    CREATE TABLE materialize_column_nost (id UInt64, base UInt64,
        a UInt64 MATERIALIZED base * 1, b UInt64 MATERIALIZED base * 2,
        c UInt64 MATERIALIZED base * 3, d UInt64 MATERIALIZED base * 4,
        e UInt64 MATERIALIZED base * 5, f UInt64 MATERIALIZED base * 6,
        INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             packed_skip_index_max_bytes = 0, auto_statistics_types = '',
             storage_policy = 'default';

    /* The preserving baseline, same shape, mutated by a DROP INDEX instead. */
    CREATE TABLE materialize_column_keep_st AS materialize_column_st;
    CREATE TABLE materialize_column_keep_nost AS materialize_column_nost;

    INSERT INTO materialize_column_st        SELECT number, number FROM numbers(50000);
    INSERT INTO materialize_column_nost      SELECT number, number FROM numbers(50000);
    INSERT INTO materialize_column_keep_st   SELECT number, number FROM numbers(50000);
    INSERT INTO materialize_column_keep_nost SELECT number, number FROM numbers(50000);
    OPTIMIZE TABLE materialize_column_st FINAL;
    OPTIMIZE TABLE materialize_column_nost FINAL;
    OPTIMIZE TABLE materialize_column_keep_st FINAL;
    OPTIMIZE TABLE materialize_column_keep_nost FINAL;

    ALTER TABLE materialize_column_st        MATERIALIZE COLUMN a SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE materialize_column_nost      MATERIALIZE COLUMN a SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE materialize_column_keep_st   DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;
    ALTER TABLE materialize_column_keep_nost DROP INDEX idx_id SETTINGS alter_sync = 2, mutations_sync = 2;

    SYSTEM FLUSH LOGS part_log;

    SELECT 'materialize_column', 'rewrites_more_than_preserving',
        toInt64(sumIf(bytes, table = 'materialize_column_st'))
                - toInt64(sumIf(bytes, table = 'materialize_column_nost'))
            > toInt64(sumIf(bytes, table = 'materialize_column_keep_st'))
                - toInt64(sumIf(bytes, table = 'materialize_column_keep_nost'))
    FROM (
        SELECT table, ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS bytes
        FROM system.part_log
        WHERE database = currentDatabase() AND event_type = 'MutatePart'
          AND table IN ('materialize_column_st', 'materialize_column_nost',
                        'materialize_column_keep_st', 'materialize_column_keep_nost')
    );
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE materialize_column_st" | while read -r line; do
    echo "materialize_column	check	$line"
done

# A column whose name merely starts with the statistics file prefix must not matter. The fix must not
# grow a name-based test; this is the case that fails the moment one is added.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE statistics_named_column (id UInt64, statistics_x String,
        INDEX idx_id id TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 0;
    INSERT INTO statistics_named_column SELECT number, repeat('y', 100) FROM numbers(20000);
    OPTIMIZE TABLE statistics_named_column FINAL;
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    ALTER TABLE statistics_named_column DROP INDEX idx_id SETTINGS alter_sync = 0;
"
wait_for_mutation "statistics_named_column" "mutation_2.txt"
$CLICKHOUSE_CLIENT --query "
    SELECT 'statistics_named_column', 'pending', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = 'statistics_named_column' AND NOT is_done;
    SELECT 'statistics_named_column', 'rows', count() FROM statistics_named_column;
"
$CLICKHOUSE_CLIENT --query "CHECK TABLE statistics_named_column" | while read -r line; do
    echo "statistics_named_column	check	$line"
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;
    DROP TABLE mutation_delete SYNC;
    DROP TABLE drop_index_compact SYNC;
    DROP TABLE drop_index_packed SYNC;
    DROP TABLE drop_index_copying SYNC;
    DROP TABLE clear_index_oversized SYNC;
    DROP TABLE drop_statistics_oversized SYNC;
    DROP TABLE drop_index_throttled SYNC;
    DROP TABLE keep_statistics SYNC;
    DROP TABLE write_footprint_keep SYNC;
    DROP TABLE write_footprint_rewrite SYNC;
    DROP TABLE write_footprint_nonexempt SYNC;
    DROP TABLE write_footprint_nonexempt_baseline SYNC;
    DROP TABLE materialize_column_st SYNC;
    DROP TABLE materialize_column_nost SYNC;
    DROP TABLE materialize_column_keep_st SYNC;
    DROP TABLE materialize_column_keep_nost SYNC;
    DROP TABLE statistics_named_column SYNC;
"
