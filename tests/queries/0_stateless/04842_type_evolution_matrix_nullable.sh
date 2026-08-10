#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Systematic sweep of the `T` -> `Nullable(T)` slice of the type-evolution matrix:
# {column spelling} x {part state after the ALTER} x {projection shape of the read}.
# A part written before a `MODIFY COLUMN` that added a `Nullable` wrapper stores non-nullable
# data, so until the mutation rewrites it, reads resolve the column with the part's own type
# while metadata (and any subcolumn entry) carries the wrapped type. Bugs in this family are
# order- and state-sensitive (see #113225 and #113925), so every cell is enumerated instead of
# sampling a few by hand.
#
# Part states:
#   compact / wide / noshare - the mutation is left unapplied on purpose (`SYSTEM STOP MERGES`
#     before an `alter_sync = 0` ALTER), so every read sees the unrewritten part;
#   rewritten - `mutations_sync = 2`, the part already carries the new type (ground truth);
#   mixed - one unrewritten part plus one part inserted after the ALTER, read in one query.
# The CREATE pins `min_*_for_wide_part` and `share_nested_offsets` because the test runner
# randomizes them and each cell needs exactly one part kind and one offsets mode.

function state_settings()
{
    case $1 in
        compact|rewritten|mixed) echo "min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1" ;;
        wide)                    echo "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1" ;;
        noshare)                 echo "min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 0" ;;
    esac
}

function state_stop_merges()
{
    case $1 in
        rewritten) echo "" ;;
        *)         echo "SYSTEM STOP MERGES $2;" ;;
    esac
}

function state_alter_settings()
{
    case $1 in
        rewritten) echo "mutations_sync = 2" ;;
        *)         echo "alter_sync = 0" ;;
    esac
}

# Every cell first prints the types the active parts actually store for the column under test.
# This is the precondition of the whole matrix: an unrewritten cell must still show the *old*
# type, a rewritten cell the new one, and a mixed cell both. Without it a change that let the
# mutation run despite `SYSTEM STOP MERGES` would silently turn the unrewritten cells into
# duplicates of the rewritten control while keeping the test green.
function state_check()
{
    echo "SELECT '-- $3: part column types';
        SELECT type, count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '$1' AND active AND column = '$2'
        GROUP BY type ORDER BY type;"
}

STATES="compact wide noshare rewritten mixed"

# A dotted member of a flattened Nested group, with a sibling member sharing the offsets.
for state in $STATES; do
    table="t_matrix_nullable_dotted_${state}"
    post_insert=""
    # `ORDER BY ALL` sorts by the selected expressions, so the two-part `mixed` cell is
    # deterministic without pulling an extra column into the read and changing its shape.
    ord=""
    if [ "$state" = "mixed" ]; then
        post_insert="INSERT INTO $table VALUES (2, [NULL, 'c'], [30, 40]);"
        ord="ORDER BY ALL"
    fi
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(String), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(String)) SETTINGS $(state_alter_settings "$state");
        $post_insert
        $(state_check "$table" "arr.n" "nullable dotted $state")
        SELECT '-- nullable dotted $state: subcolumn alone';
        SELECT \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: subcolumn, parent';
        SELECT \`arr.n\`.null, \`arr.n\` FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: parent, subcolumn';
        SELECT \`arr.n\`, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: subcolumn, sibling member';
        SELECT \`arr.n\`.null, \`arr.i\` FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: sibling member, subcolumn';
        SELECT \`arr.i\`, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: subcolumn, unrelated scalar';
        SELECT \`arr.n\`.null, id FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable dotted $state: size0, null';
        SELECT \`arr.n\`.size0, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# The same member spelled as a declared Nested type. `flatten_nested = 1` is pinned because the
# per-member ALTER below only exists in the flattened representation.
for state in $STATES; do
    table="t_matrix_nullable_nested_${state}"
    post_insert=""
    ord=""
    if [ "$state" = "mixed" ]; then
        post_insert="INSERT INTO $table VALUES (2, [NULL, 'c'], [30, 40]);"
        ord="ORDER BY ALL"
    fi
    $CLICKHOUSE_CLIENT -q "
        SET flatten_nested = 1;
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, arr Nested(n String, i UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(String)) SETTINGS $(state_alter_settings "$state");
        $post_insert
        $(state_check "$table" "arr.n" "nullable declared $state")
        SELECT '-- nullable declared $state: subcolumn alone';
        SELECT \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: subcolumn, parent';
        SELECT \`arr.n\`.null, \`arr.n\` FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: parent, subcolumn';
        SELECT \`arr.n\`, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: subcolumn, sibling member';
        SELECT \`arr.n\`.null, \`arr.i\` FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: sibling member, subcolumn';
        SELECT \`arr.i\`, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: subcolumn, unrelated scalar';
        SELECT \`arr.n\`.null, id FROM $table $ord SETTINGS max_threads = 1;
        SELECT '-- nullable declared $state: size0, null';
        SELECT \`arr.n\`.size0, \`arr.n\`.null FROM $table $ord SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# Control: an undotted Array column never joins a Nested group, so only the plain
# unrewritten-part conversion is exercised.
for state in compact wide; do
    table="t_matrix_nullable_plain_${state}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, plain Array(String))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b']);
        SYSTEM STOP MERGES $table;
        ALTER TABLE $table MODIFY COLUMN plain Array(Nullable(String)) SETTINGS alter_sync = 0;
        $(state_check "$table" "plain" "nullable plain $state")
        SELECT '-- nullable plain $state: subcolumn alone';
        SELECT plain.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- nullable plain $state: subcolumn, parent';
        SELECT plain.null, plain FROM $table SETTINGS max_threads = 1;
        SELECT '-- nullable plain $state: parent, subcolumn';
        SELECT plain, plain.null FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# Control: a dotted column that is not an Array never joins a group either.
for state in compact wide; do
    table="t_matrix_nullable_scalar_${state}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`grp.v\` UInt8)
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, 7);
        SYSTEM STOP MERGES $table;
        ALTER TABLE $table MODIFY COLUMN \`grp.v\` Nullable(UInt8) SETTINGS alter_sync = 0;
        $(state_check "$table" "grp.v" "nullable scalar $state")
        SELECT '-- nullable scalar $state: subcolumn alone';
        SELECT \`grp.v\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- nullable scalar $state: subcolumn, parent';
        SELECT \`grp.v\`.null, \`grp.v\` FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done
