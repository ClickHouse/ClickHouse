#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-object-storage, no-shared-merge-tree
# Tag no-parallel: this test mutates the server-global `shadow/` directory
# Tag no-replicated-database: Unsupported type of ALTER query
# Tag no-object-storage, no-shared-merge-tree: Scenario F uses a custom local disk

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/105719.
#
# A concurrent FREEZE/UNFREEZE race (or a writer killed between `truncate(0)`
# and the subsequent write of the new value) could leave
# `<server data path>/shadow/increment.txt` at size 0. Before the fix, every
# subsequent FREEZE on the server then failed permanently with
# `ATTEMPT_TO_READ_AFTER_EOF` until an operator manually removed the file. The
# fix in `CounterInFile::add` treats an empty file the same as a missing file
# and the call site in `MergeTreeData::freezePartitionsByMatcher` walks
# `shadow/` for the maximum existing numeric backup-directory name and passes
# it as a lower bound for the recovered counter so the next FREEZE without
# `WITH NAME` does not collide with an already-allocated `shadow/<N>/`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `shadow/` directory lives at the server data root, which is the same as
# the `default` disk path. Use `system.disks` to locate it from the test
# without hard-coding any path.
DATA_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name = 'default'" | tr -d ' \n')
SHADOW_DIR="${DATA_PATH}shadow"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_freeze_empty_inc"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_freeze_empty_inc (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_freeze_empty_inc VALUES (1), (2), (3)"

# Set by Scenarios C-F; cleaned up on exit. Empty until then so the trap never
# touches a directory we did not create. Every numeric marker below is chosen
# above the current maximum existing numeric name, so it cannot collide with a
# backup another test left behind and is safe for this test to remove.
freeze_c_planted=""
freeze_c_allocated=""
freeze_d_named=""
freeze_d_allocated=""
freeze_e_small=""
freeze_e_huge=""
freeze_e_allocated=""
freeze_f_planted=""
freeze_f_allocated=""
freeze_f_disk_shadow=""

# Remove only the `shadow/<name>` directories this test created, by exact name,
# from the given shadow root. UNFREEZE removes the frozen part data but leaves an
# empty `shadow/<name>/` shell behind, so `rm -rf` the tracked name afterwards to
# avoid leaking directories on the shared server. Every name passed here was
# chosen above the current maximum (or is the unique `backup_105719_*`), so it
# can only be a directory this test owns. `${root:?}` guards against an unset
# root expanding to `rm -rf /shadow/...`.
remove_owned_backup()
{
    local root="$1" name="$2"
    [ -n "$root" ] && [ -n "$name" ] || return 0
    rm -rf "${root:?}/${name}"
}

cleanup()
{
    # Scenarios A/B: named backups on the default disk.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME 'backup_105719_a'" > /dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME 'backup_105719_b'" > /dev/null 2>&1 || true
    remove_owned_backup "$SHADOW_DIR" "backup_105719_a"
    remove_owned_backup "$SHADOW_DIR" "backup_105719_b"
    # Scenario C: planted marker + unnamed allocation.
    if [ -n "$freeze_c_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_c_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_c_allocated"
    remove_owned_backup "$SHADOW_DIR" "$freeze_c_planted"
    # Scenario D: numeric named backup + unnamed allocation.
    if [ -n "$freeze_d_named" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_d_named}'" > /dev/null 2>&1 || true
    fi
    if [ -n "$freeze_d_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_d_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_d_named"
    remove_owned_backup "$SHADOW_DIR" "$freeze_d_allocated"
    # Scenario E: a reachable marker, an oversized marker, and an unnamed allocation.
    if [ -n "$freeze_e_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_e_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_allocated"
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_small"
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_huge"
    # Scenario F: backups on a separate custom disk (its own `shadow/`).
    if [ -n "$freeze_f_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold UNFREEZE WITH NAME '${freeze_f_allocated}'" > /dev/null 2>&1 || true
    fi
    if [ -n "$freeze_f_planted" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold UNFREEZE WITH NAME '${freeze_f_planted}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$freeze_f_disk_shadow" "$freeze_f_allocated"
    remove_owned_backup "$freeze_f_disk_shadow" "$freeze_f_planted"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_freeze_cold"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_freeze_empty_inc"
    # We intentionally do not remove `shadow/increment.txt` here: it is shared
    # with other tests on the same server. Numeric subdirectories allocated by
    # this test were removed above.
}
trap cleanup EXIT

# --- Scenario A: empty `shadow/increment.txt`, no prior backup directories.
# Plant the broken state, run FREEZE, expect success. Use `&&` so that a
# non-zero exit code from the FREEZE query causes the success message to be
# skipped, which makes the test fail with a clear reference-mismatch on the
# pre-fix server (where FREEZE throws ATTEMPT_TO_READ_AFTER_EOF).
mkdir -p "$SHADOW_DIR"
: > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME 'backup_105719_a'" > /dev/null \
    && echo "Scenario A FREEZE succeeded" \
    || echo "Scenario A FREEZE FAILED"

# --- Scenario B: empty `shadow/increment.txt`, prior backup directory exists.
# We just produced `shadow/backup_105719_a/`. Empty the counter again, and
# verify the next FREEZE WITH NAME still succeeds (this is the path Slach's
# 38400-op reproducer hits in clickhouse-backup).
: > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME 'backup_105719_b'" > /dev/null \
    && echo "Scenario B FREEZE succeeded" \
    || echo "Scenario B FREEZE FAILED"

# --- Scenario C: empty `shadow/increment.txt`, prior NUMERIC backup directory,
# UNNAMED FREEZE. This is the only scenario that exercises the numeric-directory
# scan in `freezePartitionsByMatcher`: `FREEZE WITH NAME` ignores the counter,
# so Scenarios A and B would still pass even if the scan were broken. An
# unnamed FREEZE names the backup directory after the counter, so recovering an
# empty counter to 0 would make it allocate `shadow/1` and silently reuse an
# already-existing `shadow/1/`. The scan passes the maximum existing numeric
# directory as a lower bound so the recovered counter allocates `<N>+1` instead.
#
# Plant a directory one above the current maximum numeric name so the planted
# directory is fresh regardless of numeric directories left by earlier tests on
# this shared server. Restrict to names of at most 18 digits (< 10^18): such a
# value is always reachable by the Int64 counter and cannot overflow the shell
# arithmetic below, and it excludes any oversized name a `FREEZE WITH NAME` may
# have left behind.
current_max=$(ls "$SHADOW_DIR" 2>/dev/null | grep -E '^[0-9]{1,18}$' | sort -n | tail -1)
current_max=${current_max:-0}
freeze_c_planted=$((current_max + 1))

mkdir -p "$SHADOW_DIR/$freeze_c_planted"
: > "$SHADOW_DIR/increment.txt"

freeze_c_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# A correct recovery scans existing numeric directories (across all disks) and
# allocates strictly above their maximum, so it must allocate above the planted
# one. Before the fix it recovered to 0 and allocated `1`, reusing a low-numbered
# directory. The `-gt` form is robust to numeric backups other tests may hold on
# non-default disks (which legitimately raise the allocated value). It fails
# closed when allocated is empty (a collision throws).
if [ "${freeze_c_allocated:-0}" -gt "$freeze_c_planted" ] 2>/dev/null && [ -d "$SHADOW_DIR/$freeze_c_planted" ]; then
    echo "Scenario C unnamed FREEZE allocated next id"
else
    echo "Scenario C FAILED (planted=$freeze_c_planted allocated=$freeze_c_allocated)"
fi

# --- Scenario D: empty `shadow/increment.txt`, then `FREEZE WITH NAME '<numeric>'`
# followed by an UNNAMED FREEZE. A numeric named backup creates `shadow/<N>`
# without consulting the counter, so recovering an empty counter must fold that
# numeric name into the lower bound; otherwise the next unnamed FREEZE would
# allocate `<N>` again and collide with the directory the named FREEZE just
# created. Pick a numeric name far above the current maximum so the named
# directory is the unique maximum and the invariant below is unambiguous.
current_max_d=$(ls "$SHADOW_DIR" 2>/dev/null | grep -E '^[0-9]{1,18}$' | sort -n | tail -1)
current_max_d=${current_max_d:-0}
freeze_d_named=$((current_max_d + 1000))

: > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME '${freeze_d_named}'" > /dev/null 2>&1
freeze_d_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The named directory `<freeze_d_named>` is the unique maximum, so a correct
# recovery allocates strictly above it. Before the fix the recovered counter
# ignored the numeric named value and reused `<freeze_d_named>`, so the unnamed
# FREEZE either collided (DIRECTORY_ALREADY_EXISTS, leaving allocated empty) or
# returned a value at or below `<freeze_d_named>`. The `-gt` test fails closed
# when allocated is empty or non-numeric.
if [ "${freeze_d_allocated:-0}" -gt "$freeze_d_named" ] 2>/dev/null && [ -d "$SHADOW_DIR/$freeze_d_named" ]; then
    echo "Scenario D unnamed FREEZE did not reuse numeric named backup"
else
    echo "Scenario D FAILED (named=$freeze_d_named allocated=$freeze_d_allocated)"
fi

# --- Scenario E: empty `shadow/increment.txt`, a reachable numeric directory
# AND an oversized numeric directory whose name exceeds the signed counter's
# range. `shadow/9223372036854775808` (2^63) can be created by
# `FREEZE WITH NAME '9223372036854775808'` but can never be allocated by an
# unnamed FREEZE, because the counter is Int64 and the recovered next id is
# value + 1. The recovery lower bound must skip such oversized names while
# still honouring the smaller reachable maximum. If the oversized name were
# folded into the bound, the out-of-range UInt64 -> Int64 cast becomes negative,
# `std::max<Int64>(0, ...)` recovers from 0, and the next unnamed FREEZE
# reuses (overwrites) the reachable backup directory.
#
# Pick the reachable marker one above the current maximum numeric name (like
# Scenarios C/D) rather than a fixed `shadow/1`: another stateless test may
# already own a low-numbered backup, in which case a fixed marker would neither
# prove this test created it nor be safe to remove on cleanup.
current_max_e=$(ls "$SHADOW_DIR" 2>/dev/null | grep -E '^[0-9]{1,18}$' | sort -n | tail -1)
current_max_e=${current_max_e:-0}
freeze_e_small=$((current_max_e + 1))
freeze_e_huge=9223372036854775808

mkdir -p "$SHADOW_DIR/$freeze_e_small"
mkdir -p "$SHADOW_DIR/$freeze_e_huge"
: > "$SHADOW_DIR/increment.txt"

freeze_e_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# A correct recovery ignores the oversized name and allocates strictly above
# the reachable marker, so it must not reuse it. The oversized directory must
# also survive (it is unreachable, never allocated). The `-gt` test fails
# closed when allocated is empty (a collision throws).
if [ "${freeze_e_allocated:-0}" -gt "$freeze_e_small" ] 2>/dev/null \
    && [ -d "$SHADOW_DIR/$freeze_e_small" ] && [ -d "$SHADOW_DIR/$freeze_e_huge" ]; then
    echo "Scenario E unnamed FREEZE ignored oversized numeric backup"
else
    echo "Scenario E FAILED (small=$freeze_e_small huge=$freeze_e_huge allocated=$freeze_e_allocated)"
fi

# --- Scenario F: the numeric backup directory lives on a NON-default disk.
# The counter (`shadow/increment.txt`) is kept only on the default disk, but
# each part is frozen onto its OWN disk (see `DataPartStorageOnDiskBase::freeze`),
# so `shadow/<N>/` can exist on any configured disk. A recovery that scans only
# the default disk misses those and recovers a counter that is too low; the next
# unnamed FREEZE on that disk then reuses an already-allocated `shadow/<N>`. The
# fix must compute the lower bound across all configured disks.
#
# Build a table on a custom local disk, plant a numeric named backup on that
# disk far above the default disk's maximum, empty the counter, and run an
# unnamed FREEZE: it must allocate strictly above the planted name.
disks_base="${CLICKHOUSE_DISKS_FILES:-/var/lib/clickhouse/disks}"
cold_disk_path="${disks_base}/${CLICKHOUSE_TEST_UNIQUE_NAME}_freeze_cold/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_freeze_cold"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_freeze_cold (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = disk(type = local, path = '${cold_disk_path}')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_freeze_cold VALUES (1), (2), (3)"

# Resolve the custom disk's shadow directory from the part's own disk.
cold_disk_name=$(${CLICKHOUSE_CLIENT} --query "SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = 't_freeze_cold' AND active LIMIT 1")
cold_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name = '${cold_disk_name}'" | tr -d ' \n')
freeze_f_disk_shadow="${cold_disk_root}shadow"

# Plant a numeric named backup on the custom disk, far above the default disk's
# current maximum so the expected allocation is unambiguous and disk-specific.
current_max_f=$(ls "$SHADOW_DIR" 2>/dev/null | grep -E '^[0-9]{1,18}$' | sort -n | tail -1)
current_max_f=${current_max_f:-0}
freeze_f_planted=$((current_max_f + 2000))

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold FREEZE WITH NAME '${freeze_f_planted}'" > /dev/null 2>&1
: > "$SHADOW_DIR/increment.txt"

freeze_f_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The planted directory on the custom disk is the unique maximum across all
# disks, so a correct recovery allocates strictly above it. Before the multi-disk
# fix the recovery scanned only the default disk, recovered a too-low counter,
# and the unnamed FREEZE collided with (DIRECTORY_ALREADY_EXISTS) or reused the
# planted directory. The `-gt` test fails closed when allocated is empty.
if [ "${freeze_f_allocated:-0}" -gt "$freeze_f_planted" ] 2>/dev/null \
    && [ -d "${freeze_f_disk_shadow}/${freeze_f_planted}" ]; then
    echo "Scenario F unnamed FREEZE scanned the non-default disk"
else
    echo "Scenario F FAILED (planted=$freeze_f_planted allocated=$freeze_f_allocated disk=$cold_disk_name)"
fi

# Sanity: the counter is no longer empty after the fix repaired it.
if [ -s "$SHADOW_DIR/increment.txt" ]; then
    echo "increment.txt is non-empty"
else
    echo "increment.txt is STILL EMPTY"
fi
