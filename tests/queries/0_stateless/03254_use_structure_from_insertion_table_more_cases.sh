#!/usr/bin/env bash
# Tags: no-fasttest
#       ^ because of parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

tmp_file="$CUR_DIR/03254_$CLICKHOUSE_DATABASE.parquet"
$CLICKHOUSE_LOCAL --engine_file_truncate_on_insert=1 -q "
    insert into function file('$tmp_file') select 's' as x, 42 as y;

    create table a (y String, x String) engine Memory;
    insert into a select * from file('$tmp_file');
    select 'non-reordered columns', * from a;
    truncate a;
    insert into a select * from file('$tmp_file') settings use_structure_from_insertion_table_in_table_functions = 1;
    select 'reordered columns', * from a;
    drop table a;

    -- To detect whether structure from insertion table was used, we exploit a quirk in how Int128
    -- is represented in Parquet files. CH parquet writer writes Int128 as 16-byte
    -- FIXED_LEN_BYTE_ARRAY. Reader reinterprets it back to Int128 iff the Int128 type was specified
    -- in the schema. If it wasn't, schema inference outputs FixedString(16), then toInt128 tries
    -- to parse it as text.
    -- In this test, if you see 69390914038468909559069680410903523633 it means that the
    -- use_structure_from_insertion_table_in_table_functions setting did its thing (the Int128
    -- structure hint was passed to the table function). If you see 1111222233334444 it means there
    -- was no structure hint.
    insert into function file('$tmp_file') select toFixedString('1111222233334444', 16) as x, 'meow' as z;

    create table a (x Int128) engine Memory;
    insert into a select x from file('$tmp_file'); -- type hint for x should be Int128
    select 'basic', * from a;
    truncate a;
    insert into a select x from file('$tmp_file') where z = 'meow'; -- no type hints because of where (naive structure hint would miss column z)
    select 'where', * from a;
    truncate a;
    insert into a select x from file('$tmp_file') f1 join file('$tmp_file', Parquet) f2 on (f1.z = f2.z); -- no type hints because of join (naive structure hint would miss column z)
    select 'join', * from a;
    truncate a;
    insert into a select x from file('$tmp_file') where x > 0 settings use_structure_from_insertion_table_in_table_functions = 1; -- forced type hints despite where (this breaks if the 'where' uses column z)
    select 'forced where', * from a;
    truncate a;
    insert into a select x from file('$tmp_file') f1 join file('$tmp_file', Parquet) f2 on (f1.x = f2.x) settings use_structure_from_insertion_table_in_table_functions = 1; -- ditto
    select 'forced join', * from a;
    truncate a;
    insert into a select 42 from (select z from file('$tmp_file')); -- no incorrect hint for z
    select 'subquery', * from a;
    drop table a;

    create table a (x Int128, z String) engine Memory;
    insert into a select length(x), z from file('$tmp_file'); -- no type hints because of function (naive structure hint would miss column x or use incorrect type)
    select 'function', * from a;
    truncate a;
    insert into a select length(z), z from file('$tmp_file') settings use_structure_from_insertion_table_in_table_functions = 1;
    select 'forced function', * from a;
    drop table a;
"

# Also try input() table function just in case, although it uses the same code path.
# Switch from clickhouse local to clickhouse client to be able to read from table after inserting.
cat "$tmp_file" | $CLICKHOUSE_CLIENT -q "
    create table a (x Int128) engine Memory;
    insert into a select x from input('auto') format Parquet"
$CLICKHOUSE_CLIENT -q "
    select 'basic input', * from a;
    truncate table a;"
cat "$tmp_file" | $CLICKHOUSE_CLIENT -q "
    insert into a select x from input('auto') where z = 'meow' format Parquet -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}"

rm -f "$tmp_file"
