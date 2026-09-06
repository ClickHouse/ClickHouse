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
freeze_e_baseline=""
freeze_e_allocated=""
freeze_j_padded=""
freeze_j_baseline=""
freeze_j_allocated=""
freeze_g_planted=""
freeze_g_allocated=""
freeze_h_named=""
freeze_h_allocated=""
freeze_n_baseline=""
freeze_n_named=""
freeze_p_file=""
freeze_o_planted=""
freeze_o_allocated=""
freeze_m_file=""
freeze_m_baseline=""
freeze_m_allocated=""
freeze_f_planted=""
freeze_f_cold_max=""
freeze_f_allocated=""
freeze_f_disk_shadow=""

# Largest numeric backup name an unnamed FREEZE can allocate, mirroring
# `max_reachable_id` in `freezePartitionsByMatcher`: the counter is Int64, so this
# is its maximum. Names above it are ignored by the recovery scan, so the current
# maximum must be computed over exactly this range - a shorter digit filter would
# miss a valid 19-digit backup and pick a marker that already exists.
MAX_REACHABLE_ID=9223372036854775807

# Highest existing numeric backup name in the given shadow root that the recovery
# scan actually considers, or 0 when there is none. Candidates are ordered as
# strings (shortest first, then lexicographically) rather than numerically:
# `sort -n` and awk arithmetic both go through doubles, which lose precision
# around 2^63, so a name just above the reachable maximum would compare equal to
# it and win. `s ""` forces awk's string comparison, because a field that looks
# like a number is otherwise compared numerically. Names above MAX_REACHABLE_ID
# exceed what an unnamed FREEZE can allocate, so the recovery scan ignores them
# and so must this.
current_max_numeric_backup()
{
    local root="$1" name
    # Canonical decimal names only (`0`, or no leading zero), matching
    # `numeric_dir_value`: an unnamed FREEZE names the directory after the counter.
    name=$(ls "$root" 2>/dev/null | grep -E '^(0|[1-9][0-9]{0,18})$' \
        | awk -v max="$MAX_REACHABLE_ID" \
            '{ s = $0 ""; if (length(s) < length(max) || (length(s) == length(max) && s <= max)) print s }' \
        | awk '{ print length($0), $0 }' | sort -k1,1n -k2,2 | tail -1 | cut -d" " -f2)
    echo "${name:-0}"
}

# Echo `<current maximum + offset>`, refusing to overflow. Bash arithmetic is
# signed 64-bit, so adding an offset to a maximum close to MAX_REACHABLE_ID wraps
# to a negative number, which would silently turn the scenario oracles below into
# comparisons against a nonnumeric marker. There is no headroom to recover from,
# so fail loudly instead.
next_backup_name()
{
    local root="$1" offset="$2" current
    current=$(current_max_numeric_backup "$root")
    if [ "$((MAX_REACHABLE_ID - offset))" -lt "$current" ]; then
        echo "FAILED: no room for a marker $offset above $current" >&2
        return 1
    fi
    echo "$((current + offset))"
}

# Echo a fresh `<current maximum + offset>` for the scenarios that let a
# `FREEZE WITH NAME` create the directory. The existence check is what keeps the
# cleanup trap from removing a directory this test does not own.
pick_fresh_backup_name()
{
    local root="$1" offset="$2" name
    name=$(next_backup_name "$root" "$offset") || return 1
    if [ -e "$root/$name" ]; then
        echo "FAILED to pick a fresh name, $name already exists" >&2
        return 1
    fi
    echo "$name"
}

# Create `shadow/<current maximum + offset>` and echo the name, establishing this
# test's ownership of it: plain `mkdir` (no `-p`) fails if the directory already
# exists, so a name another test owns is never silently adopted and then removed
# by the cleanup trap below.
plant_owned_backup()
{
    local root="$1" offset="$2" name
    name=$(next_backup_name "$root" "$offset") || return 1
    if ! mkdir "$root/$name" 2>/dev/null; then
        echo "FAILED to plant a fresh marker $name" >&2
        return 1
    fi
    echo "$name"
}

# Plant the first of several equivalent literal names that is still free, and echo
# it. Scenarios E and J need a name of a specific SHAPE (above the reachable
# maximum, or a leading-zero spelling of a reachable one) rather than one exact
# string, so a marker leaked by an interrupted earlier run must not make the test
# unrunnable on a shared server. Ownership is still exclusive: only a `mkdir` that
# succeeds is accepted.
plant_first_free_literal_backup()
{
    local root="$1" name
    shift
    for name in "$@"; do
        if mkdir "$root/$name" 2>/dev/null; then
            echo "$name"
            return 0
        fi
    done
    echo "FAILED to plant any of the markers: $*" >&2
    return 1
}

# Remove only the `shadow/<name>` directories this test created, by exact name,
# from the given shadow root. UNFREEZE removes the frozen part data but leaves an
# empty `shadow/<name>/` shell behind, so `rm -rf` the tracked name afterwards to
# avoid leaking directories on the shared server. Every name passed here was
# either created by `plant_owned_backup` (which proves it did not exist before),
# allocated by a FREEZE in this test, or is the unique `backup_105719_*`, so it
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
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_baseline"
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_small"
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_huge"
    # Scenario J: the non-canonical marker and the id allocated beside it.
    if [ -n "$freeze_j_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_j_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_j_allocated"
    remove_owned_backup "$SHADOW_DIR" "$freeze_j_baseline"
    remove_owned_backup "$SHADOW_DIR" "$freeze_j_padded"
    # Scenario G: a named backup plus the unnamed allocation above the planted marker.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME 'backup_105719_g'" > /dev/null 2>&1 || true
    if [ -n "$freeze_g_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_g_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "backup_105719_g"
    remove_owned_backup "$SHADOW_DIR" "$freeze_g_allocated"
    remove_owned_backup "$SHADOW_DIR" "$freeze_g_planted"
    # Scenario H: a numeric named backup plus the unnamed allocation above it.
    if [ -n "$freeze_h_named" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_h_named}'" > /dev/null 2>&1 || true
    fi
    if [ -n "$freeze_h_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_h_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_h_named"
    remove_owned_backup "$SHADOW_DIR" "$freeze_h_allocated"
    # Scenario N: the baseline allocation, plus the numeric named backup whose
    # reservation it checks.
    for n in "$freeze_n_baseline" "$freeze_n_named"; do
        if [ -n "$n" ]; then
            ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${n}'" > /dev/null 2>&1 || true
        fi
        remove_owned_backup "$SHADOW_DIR" "$n"
    done
    # Scenario P: the numeric plain file, removed by exact name like every marker.
    [ -n "$freeze_p_file" ] && rm -f "${SHADOW_DIR:?}/${freeze_p_file}"
    # Scenario O: the non-numeric named backup, its planted marker and the
    # unnamed allocation that must land above it.
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME 'backup_105719_o'" > /dev/null 2>&1 || true
    if [ -n "$freeze_o_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_o_allocated}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "backup_105719_o"
    remove_owned_backup "$SHADOW_DIR" "$freeze_o_allocated"
    remove_owned_backup "$SHADOW_DIR" "$freeze_o_planted"
    # Scenario M: the numeric plain file, plus the two allocations measured
    # around it. The file is removed by exact name, like every other marker.
    for m in "$freeze_m_baseline" "$freeze_m_allocated"; do
        if [ -n "$m" ]; then
            ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${m}'" > /dev/null 2>&1 || true
        fi
        remove_owned_backup "$SHADOW_DIR" "$m"
    done
    [ -n "$freeze_m_file" ] && rm -f "${SHADOW_DIR:?}/${freeze_m_file}"
    # Scenario F spans two disks: the marker is planted on the custom disk through
    # `t_freeze_cold`, but the allocation it verifies is taken by the default-disk
    # table, so each half is released through the table that froze it and removed
    # from that table's own `shadow/` root.
    if [ -n "$freeze_f_allocated" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_f_allocated}'" > /dev/null 2>&1 || true
    fi
    if [ -n "$freeze_f_planted" ]; then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold UNFREEZE WITH NAME '${freeze_f_planted}'" > /dev/null 2>&1 || true
    fi
    remove_owned_backup "$SHADOW_DIR" "$freeze_f_allocated"
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
# UNNAMED FREEZE. This is the first scenario that exercises the numeric-directory
# scan in `freezePartitionsByMatcher` (E, I, J, M and F rely on it too): the
# non-numeric `FREEZE WITH NAME` of Scenarios A and B never consults the counter,
# so they would still pass even if the scan were broken. An
# unnamed FREEZE names the backup directory after the counter, so recovering an
# empty counter to 0 would make it allocate `shadow/1` and silently reuse an
# already-existing `shadow/1/`. The scan passes the maximum existing numeric
# directory as a lower bound so the recovered counter allocates `<N>+1` instead.
#
# Plant a directory one above the current maximum numeric name so the planted
# directory is fresh regardless of numeric directories left by earlier tests on
# this shared server.
freeze_c_planted=$(plant_owned_backup "$SHADOW_DIR" 1)
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
freeze_d_named=$(pick_fresh_backup_name "$SHADOW_DIR" 1000)

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
freeze_e_small=$(plant_owned_backup "$SHADOW_DIR" 1)

# Establish the value a recovery that ignores the oversized name must produce, by
# measuring it BEFORE that directory exists. Asserting only "allocated > small"
# would also pass for an implementation that folds the oversized name in by
# saturating it to the counter limit, which would exhaust the namespace;
# requiring the exact same allocation with and without the directory is what pins
# the ignore contract.
: > "$SHADOW_DIR/increment.txt"
freeze_e_baseline=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)
if [ -n "$freeze_e_baseline" ]; then
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_e_baseline}'" > /dev/null 2>&1 || true
    remove_owned_backup "$SHADOW_DIR" "$freeze_e_baseline"
fi

# 2^63, one above the largest id an unnamed FREEZE can allocate. Created only now,
# and exclusively, so the baseline above provably did not see it. Continuing with
# an empty variable would make the directory assertions below address `shadow/`
# itself, so ownership of the marker is what the scenario rests on. Any of these
# names is equally oversized, so a marker leaked by an interrupted earlier run
# only makes the scenario pick the next one instead of failing outright.
# Every candidate must be ABOVE the Int64 maximum and still WITHIN UInt64: a value
# wider than UInt64 makes `stoull` throw on its own, so it would pass even with the
# explicit range check removed and the case would prove nothing.
if ! freeze_e_huge=$(plant_first_free_literal_backup "$SHADOW_DIR" \
        9223372036854775808 9223372036854775809 9223372036854775810 \
        18446744073709551614 18446744073709551615); then
    echo "Scenario E FAILED (cannot own an oversized marker)"
    exit 1
fi
: > "$SHADOW_DIR/increment.txt"

freeze_e_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The oversized name must make no difference at all: the allocation has to match
# the baseline measured without it, stay above the reachable marker, and leave
# both planted directories in place (the oversized one is never allocated). The
# `-gt`/`-eq` tests fail closed when a value is empty (a collision throws).
if [ "${freeze_e_allocated:-0}" -gt "$freeze_e_small" ] 2>/dev/null \
    && [ -n "$freeze_e_baseline" ] && [ "${freeze_e_allocated:-0}" -eq "$freeze_e_baseline" ] 2>/dev/null \
    && [ -d "$SHADOW_DIR/$freeze_e_small" ] && [ -d "$SHADOW_DIR/$freeze_e_huge" ]; then
    echo "Scenario E unnamed FREEZE ignored oversized numeric backup"
else
    echo "Scenario E FAILED (small=$freeze_e_small huge=$freeze_e_huge baseline=$freeze_e_baseline allocated=$freeze_e_allocated)"
fi

# The Int64 boundary itself (`shadow/9223372036854775807`) is covered by
# `test_freeze_recovery_refuses_exhausted_namespace` in
# tests/integration/test_freeze_recover_broken_disk, not here: that name is the
# ONE marker whose shape cannot be varied, so on this shared server a copy leaked
# by an interrupted run would make it unplantable, and while it exists every
# recovery correctly refuses, which silently breaks the scenarios below.

# --- Scenario J: a non-canonical numeric name must be ignored entirely. An
# unnamed FREEZE names its directory `toString(counter)`, so `09223372036854775807`
# is a path only `FREEZE WITH NAME` can create and the canonical
# `9223372036854775807` stays free. Parsing it as a number instead would fold the
# counter maximum into the recovery bound and make every unnamed FREEZE report an
# exhausted namespace forever, even though no reachable id is taken.
# Baseline first, as in Scenario E: requiring the same allocation with and without
# the padded directory is what proves it contributes nothing. A bare "allocated > 0"
# would also accept an implementation that parses the padded name and clamps it.
: > "$SHADOW_DIR/increment.txt"
freeze_j_baseline=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)
if [ -n "$freeze_j_baseline" ]; then
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_j_baseline}'" > /dev/null 2>&1 || true
    remove_owned_backup "$SHADOW_DIR" "$freeze_j_baseline"
fi

# Same ownership precondition as Scenario E: without the marker this scenario
# asserts nothing, so a leaked directory would otherwise turn the checks below
# into assertions about `shadow/` itself. Every candidate is a leading-zero
# spelling of a reachable id, which is the property under test, so any of them
# serves and a leaked one is simply skipped.
# Every candidate must PARSE to the Int64 maximum, only its number of leading zeros
# differing. A short spelling such as `01` parses to a value BELOW the numeric
# backups earlier scenarios already created, so folding it in would not move the
# bound and the equality oracle below would stay green with the guard removed.
if ! freeze_j_padded=$(plant_first_free_literal_backup "$SHADOW_DIR" \
        09223372036854775807 009223372036854775807 0009223372036854775807 \
        00009223372036854775807 000009223372036854775807); then
    echo "Scenario J FAILED (cannot own a leading-zero marker)"
    exit 1
fi
: > "$SHADOW_DIR/increment.txt"

freeze_j_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The FREEZE must succeed with exactly the baseline allocation, and the padded
# directory must survive.
if [ -n "$freeze_j_baseline" ] && [ "${freeze_j_allocated:-0}" -eq "$freeze_j_baseline" ] 2>/dev/null \
    && [ -d "$SHADOW_DIR/$freeze_j_padded" ]; then
    echo "Scenario J unnamed FREEZE ignored a non-canonical numeric backup"
else
    echo "Scenario J FAILED (padded=$freeze_j_padded baseline=$freeze_j_baseline allocated=$freeze_j_allocated)"
fi

# --- Scenario G: a NON-NUMERIC `FREEZE WITH NAME` must not consume the pending
# recovery. Such a name is not one an unnamed FREEZE can allocate, so there is
# nothing to reserve and the counter must be left alone. If it instead repaired the
# counter without running the scan, the next unnamed FREEZE would see a healthy
# counter, skip recovery entirely and allocate an id an existing numeric backup
# already owns. A NUMERIC name is the other half of the split and does reserve, so
# it does recover the counter - Scenarios N and P cover that.
freeze_g_planted=$(plant_owned_backup "$SHADOW_DIR" 3000)
: > "$SHADOW_DIR/increment.txt"

# Named FREEZE while the counter is empty, then an unnamed FREEZE WITHOUT
# re-emptying it: recovery must still be pending for the unnamed one.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME 'backup_105719_g'" > /dev/null 2>&1
freeze_g_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

if [ "${freeze_g_allocated:-0}" -gt "$freeze_g_planted" ] 2>/dev/null && [ -d "$SHADOW_DIR/$freeze_g_planted" ]; then
    echo "Scenario G named FREEZE left recovery pending"
else
    echo "Scenario G FAILED (planted=$freeze_g_planted allocated=$freeze_g_allocated)"
fi

# --- Scenario H: a named FREEZE still reserves a numeric name against a HEALTHY
# counter. With the counter at `k`, `FREEZE WITH NAME '<k+1>'` creates the very
# directory an unnamed FREEZE would allocate next, so it must consume that value.
# Otherwise the following unnamed FREEZE picks the same `shadow/<k+1>`: the same
# table throws DIRECTORY_ALREADY_EXISTS, and a different table silently adds its
# own data under the identifier the named backup already owns.
# Park the counter just below a fresh numeric name so the named FREEZE below is
# asking for exactly the next value the counter would hand out.
freeze_h_named=$(pick_fresh_backup_name "$SHADOW_DIR" 4000)
echo $((freeze_h_named - 1)) > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME '${freeze_h_named}'" > /dev/null 2>&1
freeze_h_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

if [ "${freeze_h_allocated:-0}" -gt "$freeze_h_named" ] 2>/dev/null && [ -d "$SHADOW_DIR/$freeze_h_named" ]; then
    echo "Scenario H named FREEZE reserved its numeric name"
else
    echo "Scenario H FAILED (named=$freeze_h_named allocated=$freeze_h_allocated)"
fi

# --- Scenario N: a named FREEZE must reserve a numeric name against an EMPTY
# counter too, not only a healthy one (Scenario H). The counter lock is released
# before the backup directory is created, so a concurrent unnamed FREEZE observes
# `shadow/` WITHOUT that directory: if the named query left the counter empty, the
# unnamed one recovers from the same state, picks the same name, and both write
# into one backup. Scenario D cannot see this, because it runs its unnamed FREEZE
# only AFTER the named directory exists, when the scan does fold it into the bound.
#
# Assert on the counter rather than on a second allocation: the value the named
# query leaves behind IS the state a concurrent unnamed FREEZE would read, so
# requiring it to be at the reserved name pins the reservation without having to
# interleave two queries.
#
# The name has to clear the maximum across ALL disks, not just the default root:
# recovery floors the counter at the name AND at the global scan maximum, so a
# higher numeric backup on any other configured disk (`cold`, the encrypted pair,
# a disk another test left registered) would legitimately leave the counter above
# the name and an exact-equality oracle would then reject correct behaviour.
# Derive it from a baseline allocation as Scenarios E, J and M do: an unnamed
# FREEZE on an empty counter recovers to the global maximum plus one, so a name
# above that value is above every existing numeric backup everywhere.
: > "$SHADOW_DIR/increment.txt"
freeze_n_baseline=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)
if [ -n "$freeze_n_baseline" ]; then
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_n_baseline}'" > /dev/null 2>&1 || true
    remove_owned_backup "$SHADOW_DIR" "$freeze_n_baseline"
fi
if [ -z "$freeze_n_baseline" ] || [ "$((MAX_REACHABLE_ID - 5000))" -lt "$freeze_n_baseline" ] 2>/dev/null; then
    echo "Scenario N FAILED to measure a global baseline ($freeze_n_baseline)"
    exit 1
fi
freeze_n_named=$((freeze_n_baseline + 5000))
if [ -e "$SHADOW_DIR/$freeze_n_named" ]; then
    echo "Scenario N FAILED to pick a fresh name, $freeze_n_named already exists"
    exit 1
fi
: > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME '${freeze_n_named}'" > /dev/null 2>&1
freeze_n_counter=$(tr -d ' \n' < "$SHADOW_DIR/increment.txt" 2>/dev/null)

# The counter must have been recovered to exactly the reserved name, so the next
# unnamed FREEZE allocates `<name>+1`. An implementation that leaves it empty, or
# recovers it below the name, fails here. The `-eq` form fails closed when the
# counter is empty or non-numeric, and the directory check requires the named
# FREEZE itself to have succeeded.
if [ "${freeze_n_counter:-0}" -eq "$freeze_n_named" ] 2>/dev/null && [ -d "$SHADOW_DIR/$freeze_n_named" ]; then
    echo "Scenario N named FREEZE reserved its numeric name on an empty counter"
else
    echo "Scenario N FAILED (named=$freeze_n_named counter=$freeze_n_counter)"
fi

# --- Scenario P: the reservation must happen BEFORE the backup directory is
# created, which is the ordering the race turns on: the counter lock is released
# first, so a concurrent unnamed FREEZE reads the counter while the directory is
# still absent. Scenario N inspects the state after the query finished, so it is
# equally satisfied by a reservation moved AFTER directory creation, which would
# reopen the window it exists to close.
#
# Pin the ordering without interleaving two queries: make the FREEZE fail at
# directory creation, and require the counter to carry the reservation anyway. A
# numeric PLAIN FILE at `shadow/<name>` does that, and the recovery scan ignores
# plain files (Scenario M), so it cannot move the bound the reservation is floored
# against.
freeze_p_named=$((freeze_n_baseline + 7000))
if [ "$((MAX_REACHABLE_ID - 7000))" -lt "$freeze_n_baseline" ] 2>/dev/null; then
    echo "Scenario P FAILED: no room for a marker above $freeze_n_baseline"
    exit 1
fi
: > "$SHADOW_DIR/increment.txt"
# `noclobber` makes the redirect fail rather than truncate, proving the name was
# free and is therefore ours to remove.
if ! (set -o noclobber; true > "$SHADOW_DIR/$freeze_p_named") 2>/dev/null; then
    echo "Scenario P FAILED to plant a numeric file ($freeze_p_named)"
    exit 1
fi
freeze_p_file="$freeze_p_named"

# Report only whether the query failed, not its message: the failure carries a
# server stack trace, which would otherwise land in this test's output.
if ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME '${freeze_p_named}'" > /dev/null 2>&1; then
    freeze_p_failed=no
else
    freeze_p_failed=yes
fi
freeze_p_counter=$(tr -d ' \n' < "$SHADOW_DIR/increment.txt" 2>/dev/null)

# The FREEZE must fail (the path is a file, so the backup directory cannot be
# created) and the counter must still hold the reservation: that combination is
# only possible if the counter was advanced before the directory was attempted.
# Moving the reservation after directory creation leaves the counter empty here.
if [ "$freeze_p_failed" = yes ] && [ "${freeze_p_counter:-0}" -eq "$freeze_p_named" ] 2>/dev/null \
    && [ -f "$SHADOW_DIR/$freeze_p_file" ]; then
    echo "Scenario P named FREEZE reserved its numeric name before creating the directory"
else
    echo "Scenario P FAILED (named=$freeze_p_named counter=$freeze_p_counter failed=$freeze_p_failed)"
fi

# --- Scenario O: a NON-numeric named FREEZE must still leave an empty counter
# untouched, so recovery stays pending for the unnamed FREEZE that owns the scan.
# Without this, Scenario N would also be satisfied by recovering the counter on
# EVERY named FREEZE, which would let the next unnamed FREEZE see a healthy
# counter, skip the scan, and reuse an existing numeric `shadow/<N>`. Scenario G
# pins the same property through a later allocation; here the counter is read
# directly, so both halves of the split are asserted the same way.
freeze_o_planted=$(plant_owned_backup "$SHADOW_DIR" 6000)
: > "$SHADOW_DIR/increment.txt"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE WITH NAME 'backup_105719_o'" > /dev/null 2>&1
freeze_o_counter_size=$(stat -c %s "$SHADOW_DIR/increment.txt" 2>/dev/null)
freeze_o_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The counter must still be empty right after the named FREEZE, and the unnamed
# FREEZE that follows must then recover above the planted numeric directory.
if [ "${freeze_o_counter_size:-1}" -eq 0 ] 2>/dev/null \
    && [ "${freeze_o_allocated:-0}" -gt "$freeze_o_planted" ] 2>/dev/null \
    && [ -d "$SHADOW_DIR/backup_105719_o" ]; then
    echo "Scenario O non-numeric named FREEZE left the empty counter alone"
else
    echo "Scenario O FAILED (planted=$freeze_o_planted counter_size=$freeze_o_counter_size allocated=$freeze_o_allocated)"
fi

# Scenarios K and L, which drive the named FREEZE against an exhausted, an
# unparsable and an unopenable counter, live in
# tests/integration/test_freeze_recover_broken_disk. Each of those states has to be
# written into `shadow/increment.txt` itself, and none of them is recoverable: this
# fix only self-heals an empty or missing counter, while the shared
# `shadow/increment.txt` is deliberately never restored by the cleanup below
# because other tests use it. A run killed inside those scenarios would therefore
# make every later FREEZE on the server fail. Each integration instance has its own
# counter, so writing those states there is safe.

# --- Scenario M: a numeric plain FILE in `shadow/` is not a backup. The
# directory iterator yields files too, so a name alone does not identify one:
# counting `shadow/<N>` when it is a file would raise the bound over identifiers
# that are still free, and a file named the counter's maximum would refuse every
# recovery outright. Measure the recovered allocation without the file and again
# with it, and require the two to be EQUAL. A `>` oracle would instead be
# satisfied by the very inflation this pins.
#
# The baseline allocation creates its own `shadow/<N>`, which would itself raise
# the bound for the second measurement, so release and remove it first: both
# measurements must start from an identical directory state for the equality to
# mean anything.
: > "$SHADOW_DIR/increment.txt"
freeze_m_baseline=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)
if [ -n "$freeze_m_baseline" ]; then
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc UNFREEZE WITH NAME '${freeze_m_baseline}'" > /dev/null 2>&1 || true
    remove_owned_backup "$SHADOW_DIR" "$freeze_m_baseline"
fi

# A numeric file well above anything the scan can legitimately see, so any effect
# on the bound is unmistakable. `next_backup_name` keeps it clear of real backups,
# and `set -o noclobber` makes the redirect fail rather than truncate, which
# proves the name was free and so is ours to remove.
freeze_m_file=$(next_backup_name "$SHADOW_DIR" 4000)
if [ -z "$freeze_m_file" ] || ! (set -o noclobber; > "$SHADOW_DIR/$freeze_m_file") 2>/dev/null; then
    echo "Scenario M FAILED to plant a numeric file ($freeze_m_file)"
    exit 1
fi

: > "$SHADOW_DIR/increment.txt"
freeze_m_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The planted name must still be a plain file: had the scan counted it, the
# allocation would have landed above it instead of back at the baseline.
if [ -n "$freeze_m_allocated" ] && [ "$freeze_m_allocated" = "$freeze_m_baseline" ] \
    && [ -f "$SHADOW_DIR/$freeze_m_file" ]; then
    echo "Scenario M unnamed FREEZE ignored a numeric plain file"
else
    echo "Scenario M FAILED (baseline=$freeze_m_baseline allocated=$freeze_m_allocated file=$freeze_m_file)"
fi

# Leave a usable counter behind for the remaining scenarios.
: > "$SHADOW_DIR/increment.txt"

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

# Plant a numeric named backup on the custom disk, above the maximum on BOTH
# roots so the expected allocation is unambiguous. The name must be fresh on the
# custom disk too, because that is where it gets created and where the cleanup
# trap removes it: checking only the default root could adopt, and then delete, a
# pre-existing backup root over there.
# The candidate must clear the maximum on BOTH roots, not just the default one:
# the assertion below requires the planted name to exceed the custom disk's own
# maximum, so deriving it from the default root alone can pick a name a correct
# global scan legitimately allocates above (default max 100, custom max 5000
# would select 2100 and then reject a correct allocation above 5000).
freeze_f_cold_max=$(current_max_numeric_backup "$freeze_f_disk_shadow")
freeze_f_default_max=$(current_max_numeric_backup "$SHADOW_DIR")
if [ "$freeze_f_cold_max" -gt "$freeze_f_default_max" ] 2>/dev/null; then
    freeze_f_base_root="$freeze_f_disk_shadow"
else
    freeze_f_base_root="$SHADOW_DIR"
fi

freeze_f_offset=2000
while :; do
    freeze_f_planted=$(next_backup_name "$freeze_f_base_root" "$freeze_f_offset") || break
    [ -e "${freeze_f_disk_shadow}/${freeze_f_planted}" ] || [ -e "${SHADOW_DIR}/${freeze_f_planted}" ] \
        || break
    freeze_f_offset=$((freeze_f_offset + 1000))
done

${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_cold FREEZE WITH NAME '${freeze_f_planted}'" > /dev/null 2>&1
: > "$SHADOW_DIR/increment.txt"

# Recover by freezing the DEFAULT-disk table: the marker lives on a disk this
# table never touches, so an implementation that scans only the current table's
# storage policy misses it and allocates at or below the planted name.
freeze_f_allocated=$(${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_freeze_empty_inc FREEZE FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1" 2>/dev/null | tail -n +2 | head -1 | cut -f4)

# The planted directory on the custom disk is the unique maximum across all
# disks, so a correct recovery allocates strictly above it. Before the multi-disk
# fix the recovery scanned only the default disk, recovered a too-low counter,
# and the unnamed FREEZE reused an already-allocated name. The `-gt` test fails
# closed when allocated is empty, and the planted marker must survive.
if [ "${freeze_f_allocated:-0}" -gt "$freeze_f_planted" ] 2>/dev/null \
    && [ "$freeze_f_planted" -gt "${freeze_f_cold_max:-0}" ] 2>/dev/null \
    && [ -d "${freeze_f_disk_shadow}/${freeze_f_planted}" ]; then
    echo "Scenario F unnamed FREEZE scanned the non-default disk"
else
    echo "Scenario F FAILED (planted=$freeze_f_planted allocated=$freeze_f_allocated disk=$cold_disk_name)"
fi

# Sanity: the last FREEZE above was unnamed, so it repaired the counter.
# (A non-numeric named FREEZE deliberately leaves it empty - see Scenarios G and O.)
if [ -s "$SHADOW_DIR/increment.txt" ]; then
    echo "increment.txt is non-empty"
else
    echo "increment.txt is STILL EMPTY"
fi
