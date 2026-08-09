#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Tuple` and `Dynamic` slices of the type-evolution matrix (see the `Nullable` slice in
# `04836_type_evolution_matrix_nullable.sh` for the design): a part written before a
# `MODIFY COLUMN` that wrapped a `Nested` member into `Tuple(...)` or `Dynamic` keeps the
# unwrapped data until the mutation rewrites it, and subcolumn reads of the member must resolve
# against the part's own type in every projection order. Also covers a depth-3 subcolumn path
# (`member.element.null`) after an ALTER that made a `Tuple` element `Nullable`.

function state_settings()
{
    case $1 in
        compact|rewritten) echo "min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, share_nested_offsets = 1" ;;
        wide)              echo "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1" ;;
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

# `Array(String)` -> `Array(Tuple(x String))`: the values are strings parseable as tuples.
for state in compact wide rewritten; do
    table="t_matrix_tuple_dotted_${state}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(String), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['(''p'')', '(''q'')'], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Tuple(x String)) SETTINGS $(state_alter_settings "$state");
        SELECT '-- tuple dotted $state: subcolumn alone';
        SELECT \`arr.n\`.x FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple dotted $state: subcolumn, parent';
        SELECT \`arr.n\`.x, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple dotted $state: parent, subcolumn';
        SELECT \`arr.n\`, \`arr.n\`.x FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple dotted $state: subcolumn, sibling member';
        SELECT \`arr.n\`.x, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple dotted $state: subcolumn, unrelated scalar';
        SELECT \`arr.n\`.x, id FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# `Array(String)` -> `Array(Dynamic)`: the member's values become `Dynamic` holding `String`,
# and the typed subcolumn `.String` reads them back as `Nullable(String)`.
for state in compact wide rewritten; do
    table="t_matrix_dynamic_dotted_${state}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(String), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Dynamic) SETTINGS $(state_alter_settings "$state");
        SELECT '-- dynamic dotted $state: typed subcolumn alone';
        SELECT \`arr.n\`.String FROM $table SETTINGS max_threads = 1;
        SELECT '-- dynamic dotted $state: typed subcolumn, parent';
        SELECT \`arr.n\`.String, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- dynamic dotted $state: parent, typed subcolumn';
        SELECT \`arr.n\`, \`arr.n\`.String FROM $table SETTINGS max_threads = 1;
        SELECT '-- dynamic dotted $state: typed subcolumn, sibling member';
        SELECT \`arr.n\`.String, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- dynamic dotted $state: absent typed subcolumn, sibling member';
        SELECT \`arr.n\`.Int64, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# Depth 3: the member is `Array(Tuple(a String, b Float64))` and the ALTER makes the element
# `a` Nullable, so `arr.n.a.null` crosses the member, the tuple element and the wrapper.
for state in compact wide rewritten; do
    table="t_matrix_tuple_deep_${state}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(Tuple(a String, b Float64)), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, [('x', 1.5), ('y', 2.5)], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Tuple(a Nullable(String), b Float64)) SETTINGS $(state_alter_settings "$state");
        SELECT '-- tuple deep $state: depth-3 subcolumn alone';
        SELECT \`arr.n\`.a.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple deep $state: depth-3 subcolumn, element';
        SELECT \`arr.n\`.a.null, \`arr.n\`.b FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple deep $state: element, depth-3 subcolumn';
        SELECT \`arr.n\`.b, \`arr.n\`.a.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple deep $state: depth-3 subcolumn, parent';
        SELECT \`arr.n\`.a.null, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- tuple deep $state: depth-3 subcolumn, sibling member';
        SELECT \`arr.n\`.a.null, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done
