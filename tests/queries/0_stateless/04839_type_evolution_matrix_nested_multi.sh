#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The multi-member slice of the type-evolution matrix (see
# `04836_type_evolution_matrix_nullable.sh` for the design): one `Nested` group whose members
# are in different evolution states at once. A shared-offsets group is synthesized from all
# requested members, so the interaction of a type-diverged member (unrewritten after a
# `MODIFY COLUMN`), a member missing from the part (dropped and re-added), and an untouched
# member is its own row of cells, not implied by the single-member slices.

function state_settings()
{
    case $1 in
        compact) echo "min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1" ;;
        wide)    echo "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1" ;;
    esac
}

# One group, three member states: `n` is type-diverged (the part stores `Array(String)`,
# metadata says `Array(Nullable(String))`), `s` is missing from the part (dropped and
# re-added), `i` is untouched.
#
# Note the part-kind asymmetry for `s`, pinned deliberately: while the DROP's mutation is
# blocked, a Wide part still has the column's files and per-file presence resurfaces the old
# values (`['s1','s2']`), while a Compact part treats the re-added column as absent and
# synthesizes defaults onto the group's shared offsets (`['','']`).
for state in compact wide; do
    table="t_matrix_multi_${state}"
    $CLICKHOUSE_CLIENT -q "
        SET flatten_nested = 1;
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, arr Nested(n String, i UInt64, s String))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20], ['s1', 's2']);
        SYSTEM STOP MERGES $table;
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(String)) SETTINGS alter_sync = 0;
        ALTER TABLE $table DROP COLUMN \`arr.s\` SETTINGS alter_sync = 0;
        ALTER TABLE $table ADD COLUMN \`arr.s\` Array(String) SETTINGS alter_sync = 0;
        SELECT '-- multi $state: diverged subcolumn, missing subcolumn, untouched member';
        SELECT \`arr.n\`.null, \`arr.s\`.size0, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi $state: missing member, diverged subcolumn';
        SELECT \`arr.s\`, \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi $state: diverged subcolumn, missing member';
        SELECT \`arr.n\`.null, \`arr.s\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi $state: missing subcolumn, diverged member, untouched member';
        SELECT \`arr.s\`.size0, \`arr.n\`, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi $state: diverged member, missing member, untouched subcolumn';
        SELECT \`arr.n\`, \`arr.s\`, \`arr.i\`.size0 FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# Two diverged members in one group, read in both orders.
for state in compact wide; do
    table="t_matrix_multi2_${state}"
    $CLICKHOUSE_CLIENT -q "
        SET flatten_nested = 1;
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, arr Nested(n String, i UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        SYSTEM STOP MERGES $table;
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(String)) SETTINGS alter_sync = 0;
        ALTER TABLE $table MODIFY COLUMN \`arr.i\` Array(Nullable(UInt64)) SETTINGS alter_sync = 0;
        SELECT '-- multi2 $state: both subcolumns';
        SELECT \`arr.n\`.null, \`arr.i\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi2 $state: both subcolumns, reversed';
        SELECT \`arr.i\`.null, \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi2 $state: subcolumn of one, parent of the other';
        SELECT \`arr.n\`.null, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi2 $state: parent of one, subcolumn of the other';
        SELECT \`arr.i\`, \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi2 $state: both parents, both subcolumns';
        SELECT \`arr.n\`, \`arr.i\`, \`arr.n\`.null, \`arr.i\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi2 $state: both subcolumns, both parents';
        SELECT \`arr.n\`.null, \`arr.i\`.null, \`arr.n\`, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# A diverged member read together with the whole declared group is not possible (the flattened
# spelling has no whole-group column), but the group's offsets can also be consumed through
# `length`, which reads only offsets of one member.
for state in compact wide; do
    table="t_matrix_multi3_${state}"
    $CLICKHOUSE_CLIENT -q "
        SET flatten_nested = 1;
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, arr Nested(n String, i UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        SYSTEM STOP MERGES $table;
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(String)) SETTINGS alter_sync = 0;
        SELECT '-- multi3 $state: length of diverged, subcolumn of diverged';
        SELECT length(\`arr.n\`), \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- multi3 $state: subcolumn of diverged, length of sibling';
        SELECT \`arr.n\`.null, length(\`arr.i\`) FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done
