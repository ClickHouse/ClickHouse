#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The widening and `LowCardinality` slices of the type-evolution matrix (see
# `04842_type_evolution_matrix_nullable.sh` for the design). These evolutions change the leaf
# column class without adding a wrapper level, so the cells pin value conversion correctness
# (an old part read through the new type must return converted values) and the `.size0`
# subcolumn, which exists for every `Array` regardless of the element type. Each cell that
# reads the subcolumn next to a sibling member also has the reversed cell where the sibling
# comes first, because the motivating bug family is projection-order-sensitive.
#
# All cells are accumulated into one `clickhouse-client` invocation: client startup dominates
# under sanitizers, and the flaky check runs the test many times in parallel with itself, so
# per-cell invocations can push a run past the per-test time limit.

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

# Every cell first prints the types the active parts actually store for the column under test:
# an unrewritten cell must still show the *old* type and a rewritten cell the new one. Without
# this precondition a change that let the mutation run despite `SYSTEM STOP MERGES` would
# silently turn the unrewritten cells into duplicates of the rewritten control and stay green.
function state_check()
{
    echo "SELECT '-- $3: part column types';
        SELECT type, count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '$1' AND active AND column = '$2'
        GROUP BY type ORDER BY type;"
}

queries=""

# `Array(UInt32)` -> `Array(UInt64)`: pure widening of a member's element.
for state in compact wide rewritten; do
    table="t_matrix_widen_dotted_${state}"
    queries+="
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(UInt32), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, [100, 200], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(UInt64) SETTINGS $(state_alter_settings "$state");
        $(state_check "$table" "arr.n" "widen dotted $state")
        SELECT '-- widen dotted $state: parent alone';
        SELECT \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: size0 alone';
        SELECT \`arr.n\`.size0 FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: size0, parent';
        SELECT \`arr.n\`.size0, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: parent, size0';
        SELECT \`arr.n\`, \`arr.n\`.size0 FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: size0, sibling member';
        SELECT \`arr.n\`.size0, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: sibling member, size0';
        SELECT \`arr.i\`, \`arr.n\`.size0 FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen dotted $state: type of the read parent';
        SELECT toTypeName(\`arr.n\`) FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# `Array(UInt32)` -> `Array(Nullable(UInt64))`: widening combined with a wrapper.
for state in compact wide rewritten; do
    table="t_matrix_widen_null_dotted_${state}"
    queries+="
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(UInt32), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, [100, 200], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(Nullable(UInt64)) SETTINGS $(state_alter_settings "$state");
        $(state_check "$table" "arr.n" "widen+nullable dotted $state")
        SELECT '-- widen+nullable dotted $state: subcolumn alone';
        SELECT \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen+nullable dotted $state: subcolumn, parent';
        SELECT \`arr.n\`.null, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen+nullable dotted $state: parent, subcolumn';
        SELECT \`arr.n\`, \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen+nullable dotted $state: subcolumn, sibling member';
        SELECT \`arr.n\`.null, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- widen+nullable dotted $state: sibling member, subcolumn';
        SELECT \`arr.i\`, \`arr.n\`.null FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# `Array(String)` -> `Array(LowCardinality(String))`: the wrapper changes the column class of
# the leaf without adding a serialization level with its own subcolumns.
for state in compact wide rewritten; do
    table="t_matrix_lc_dotted_${state}"
    queries+="
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt8, \`arr.n\` Array(String), \`arr.i\` Array(UInt64))
        ENGINE = MergeTree ORDER BY id
        SETTINGS $(state_settings "$state"), auto_statistics_types = '';
        INSERT INTO $table VALUES (1, ['a', 'b'], [10, 20]);
        $(state_stop_merges "$state" "$table")
        ALTER TABLE $table MODIFY COLUMN \`arr.n\` Array(LowCardinality(String)) SETTINGS $(state_alter_settings "$state");
        $(state_check "$table" "arr.n" "lowcardinality dotted $state")
        SELECT '-- lowcardinality dotted $state: parent alone';
        SELECT \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- lowcardinality dotted $state: size0, parent';
        SELECT \`arr.n\`.size0, \`arr.n\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- lowcardinality dotted $state: parent, size0';
        SELECT \`arr.n\`, \`arr.n\`.size0 FROM $table SETTINGS max_threads = 1;
        SELECT '-- lowcardinality dotted $state: size0, sibling member';
        SELECT \`arr.n\`.size0, \`arr.i\` FROM $table SETTINGS max_threads = 1;
        SELECT '-- lowcardinality dotted $state: sibling member, size0';
        SELECT \`arr.i\`, \`arr.n\`.size0 FROM $table SETTINGS max_threads = 1;
        DROP TABLE $table;
    "
done

# Control: an undotted scalar widening never involves a Nested group.
queries+="
    DROP TABLE IF EXISTS t_matrix_widen_scalar;
    CREATE TABLE t_matrix_widen_scalar (id UInt8, x UInt32)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 1048576, auto_statistics_types = '';
    INSERT INTO t_matrix_widen_scalar VALUES (1, 100);
    SYSTEM STOP MERGES t_matrix_widen_scalar;
    ALTER TABLE t_matrix_widen_scalar MODIFY COLUMN x UInt64 SETTINGS alter_sync = 0;
    $(state_check "t_matrix_widen_scalar" "x" "widen scalar control")
    SELECT '-- widen scalar control';
    SELECT x, toTypeName(x) FROM t_matrix_widen_scalar SETTINGS max_threads = 1;
    DROP TABLE t_matrix_widen_scalar;
"

$CLICKHOUSE_CLIENT -q "$queries"
