#!/usr/bin/env bash
# Tags: no-parallel-replicas, no-replicated-database, no-shared-catalog
# Tag no-replicated-database / no-shared-catalog: the test edits on-disk table
# metadata, which would break the metadata digest of a Replicated database.
#
# Regression for issue #102445 plus the surrounding checkProperties projection gate contract.
#
# checkProperties gates a projection that uses a virtual column behind the feature setting that
# enables it. Two distinct gate classes:
#   - allow_part_offset_column_in_projections / allow_commit_order_projection are pure CREATE-time
#     gates (nothing at merge / MATERIALIZE PROJECTION time reads them). They must fire only when
#     THIS operation is responsible for the invalid pairing: a projection introduced by the current
#     operation (CREATE / ADD PROJECTION) with the gate off, or an ALTER that flips the gate from
#     enabled to disabled while such a projection already exists. They must NOT fire on ATTACH
#     (else a table becomes permanently unattachable once the default flips across versions, the
#     #102445 bug) nor for an unrelated later ALTER that leaves an already-disabled gate untouched.
#   - enable_block_number_column / enable_block_offset_column are NOT CREATE-only: a commit-order
#     projection can be rebuilt from the base part during a merge, and that rebuild produces
#     _block_number / _block_offset only when these settings are enabled. So they stay validated
#     for every projection (even pre-existing, even on ATTACH) against the effective post-operation
#     settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")

# Turns a CREATE-only gate off directly in the on-disk metadata (simulating a cross-version default
# flip) and re-attaches, exercising the #102445 ATTACH path without an ALTER (which is rejected, see
# the flip cases below).
attach_with_gate_disabled() {
    local table=$1 setting=$2
    local rel_path meta
    rel_path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables WHERE database = currentDatabase() AND name = '$table'")
    meta="$data_path$rel_path"
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE $table"
    # sed exits 0 even when it rewrites nothing, so assert the enabled spelling exists before the edit
    # and the disabled spelling exists after it. Otherwise a SHOW CREATE formatting drift away from
    # "$setting = 1" would leave the metadata untouched and re-attach it -- a false positive.
    grep -q "$setting = 1" "$meta" || { echo "FAIL: '$setting = 1' not found in $table metadata before rewrite"; return 1; }
    sed -i "s/$setting = 1/$setting = 0/" "$meta"
    grep -q "$setting = 0" "$meta" || { echo "FAIL: '$setting = 0' not present in $table metadata after rewrite"; return 1; }
    ${CLICKHOUSE_CLIENT} -q "ATTACH TABLE $table"
}

# (1) _part_offset projection: allow_part_offset_column_in_projections is CREATE-only, so a table
# whose on-disk metadata has it disabled must still ATTACH (issue #102445).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_po SYNC;
    CREATE TABLE t_04545_po (a UInt64, b UInt64,
        PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
    ENGINE = MergeTree ORDER BY a
    SETTINGS allow_part_offset_column_in_projections = 1;
    INSERT INTO t_04545_po VALUES (1, 1), (2, 2);
"
attach_with_gate_disabled t_04545_po allow_part_offset_column_in_projections
${CLICKHOUSE_CLIENT} -q "SELECT '_part_offset attach', count() FROM t_04545_po;"

# (2) commit-order projection: allow_commit_order_projection is CREATE-only -> ATTACH must succeed
# even with the gate disabled on disk.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_co SYNC;
    CREATE TABLE t_04545_co (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    INSERT INTO t_04545_co(a) VALUES (1), (2);
"
attach_with_gate_disabled t_04545_co allow_commit_order_projection
${CLICKHOUSE_CLIENT} -q "SELECT 'commit_order attach', count() FROM t_04545_co;"

# (3) after the #102445 attach, an unrelated later ALTER (ADD COLUMN) must NOT be rejected: no new
# projection is introduced and the already-off gate is untouched.
${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE t_04545_po ADD COLUMN c UInt64;
    SELECT 'unrelated alter after attach ok';
"

# (4) an ALTER that flips a CREATE-only gate from enabled to disabled while a matching projection
# already exists must be rejected (matches PR #104822's regression 04313): a table with such a
# projection must not be able to turn the feature off.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_flip_po SYNC;
    CREATE TABLE t_04545_flip_po (a UInt64, b UInt64,
        PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
    ENGINE = MergeTree ORDER BY a
    SETTINGS allow_part_offset_column_in_projections = 1;
    ALTER TABLE t_04545_flip_po MODIFY SETTING allow_part_offset_column_in_projections = 0;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING part_offset flip rejection"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_flip_co SYNC;
    CREATE TABLE t_04545_flip_co (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    ALTER TABLE t_04545_flip_co MODIFY SETTING allow_commit_order_projection = 0;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING commit_order flip rejection"

# (5) enable_block_number_column / enable_block_offset_column are merge-time dependencies of a
# commit-order projection, so disabling them via ALTER while such a projection exists must be
# rejected up front (otherwise a later merge / MATERIALIZE PROJECTION rebuild runs without
# materializing the required _block_number / _block_offset).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_bn SYNC;
    CREATE TABLE t_04545_bn (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    ALTER TABLE t_04545_bn MODIFY SETTING enable_block_number_column = 0;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING block_number modify rejection"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_bo SYNC;
    CREATE TABLE t_04545_bo (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    ALTER TABLE t_04545_bo MODIFY SETTING enable_block_offset_column = 0;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING block_offset modify rejection"

# (6) RESET SETTING drops the key from the override list, so the effective value must fall back to
# the DEFAULT (0 here). checkProperties validates against getDefaultSettings() + settings_changes,
# so RESET of these merge-time settings while a commit-order projection exists is also rejected.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_rbn SYNC;
    CREATE TABLE t_04545_rbn (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    ALTER TABLE t_04545_rbn RESET SETTING enable_block_number_column;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING block_number reset rejection"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_rbo SYNC;
    CREATE TABLE t_04545_rbo (a UInt64,
        PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
    ALTER TABLE t_04545_rbo RESET SETTING enable_block_offset_column;
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING block_offset reset rejection"

# (7) control: disabling enable_block_number_column on a table WITHOUT a commit-order projection
# stays allowed (nothing depends on the column).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_plain SYNC;
    CREATE TABLE t_04545_plain (a UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1;
    ALTER TABLE t_04545_plain MODIFY SETTING enable_block_number_column = 0;
    SELECT 'plain disable ok';
"

# (8) a mixed ALTER that both ADDs a commit-order projection and enables the gate must succeed: the
# gate is validated against the effective post-ALTER settings, not the stale live value.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_mix SYNC;
    CREATE TABLE t_04545_mix (a UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    ALTER TABLE t_04545_mix
        ADD PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset),
        MODIFY SETTING allow_commit_order_projection = 1;
    SELECT 'mixed add-projection enable-gate ok';
"

# (9) control: ADD PROJECTION introducing a commit-order projection while the gate is still off must
# be rejected (the gate still fires for a projection introduced by the current operation).
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_addoff SYNC;
    CREATE TABLE t_04545_addoff (a UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 0;
    ALTER TABLE t_04545_addoff ADD PROJECTION p (SELECT a, _block_number ORDER BY _block_number);
" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "MISSING add-projection gate-off rejection"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_04545_po SYNC;
    DROP TABLE IF EXISTS t_04545_co SYNC;
    DROP TABLE IF EXISTS t_04545_flip_po SYNC;
    DROP TABLE IF EXISTS t_04545_flip_co SYNC;
    DROP TABLE IF EXISTS t_04545_bn SYNC;
    DROP TABLE IF EXISTS t_04545_bo SYNC;
    DROP TABLE IF EXISTS t_04545_rbn SYNC;
    DROP TABLE IF EXISTS t_04545_rbo SYNC;
    DROP TABLE IF EXISTS t_04545_plain SYNC;
    DROP TABLE IF EXISTS t_04545_mix SYNC;
    DROP TABLE IF EXISTS t_04545_addoff SYNC;
"
