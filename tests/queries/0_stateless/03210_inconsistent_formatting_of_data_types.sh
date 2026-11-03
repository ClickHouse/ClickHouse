#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Ensure that these (possibly incorrect) queries can at least be parsed back after formatting.
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE columns_with_multiple_streams MODIFY COLUMN field1 Nullable(tupleElement(x, 2), UInt8)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t_update_empty_nested ADD COLUMN \`nested.arr2\` Array(tuple('- ON NULL -', toLowCardinality(11), 11, 11, toLowCardinality(11), 11), UInt64)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE enum_alter_issue (MODIFY COLUMN a Enum8(equals('one', timeSlots(timeSlots(arrayEnumerateDense(tuple('0.2147483646', toLowCardinality(toUInt128(12))), NULL), 4, 12.34, materialize(73), 2)), 1)))" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t_sparse_mutations_3 MODIFY COLUMN s Tuple(Nullable(tupleElement(s, 1), UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(String))" | $CLICKHOUSE_FORMAT --oneline

# A Tuple type that mixes named and unnamed elements (e.g. produced by the AST
# fuzzer) must still round-trip through formatting. The unnamed element used to
# be formatted as an empty backquoted identifier `` which could not be parsed
# back, breaking the round-trip (STID 1941-26fa).
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t (x Tuple(a UInt8, UInt16)) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t (\`t\` Tuple(JSON, f1 Array(Float64))) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t MODIFY COLUMN s Tuple(a UInt8, UInt16)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t (x Tuple(a Tuple(b UInt8, UInt16), c String)) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline

# Same mixed named/unnamed tuples, but formatted without --oneline so the first
# pass takes the pretty (multi-line) branch of ASTTupleDataType::formatImpl
# (print_pretty_type_names = true, more than one element). The multi-line output
# must reparse cleanly; re-format it with --oneline to assert a stable result.
$CLICKHOUSE_FORMAT --query "CREATE TABLE t (x Tuple(a UInt8, UInt16)) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --query "CREATE TABLE t (\`t\` Tuple(JSON, f1 Array(Float64))) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --query "ALTER TABLE t MODIFY COLUMN s Tuple(a UInt8, UInt16)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --query "CREATE TABLE t (x Tuple(a Tuple(b UInt8, UInt16), c String)) ENGINE = Memory" | $CLICKHOUSE_FORMAT --oneline

# These invalid queries don't parse and this is normal.
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE alter_compression_codec1 MODIFY COLUMN alter_column CODEC((2 + ignore(1, toUInt128(materialize(2)), 2 + toNullable(toNullable(3))), 3), NONE)" 2>&1 | grep -o -F 'Syntax error'
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE test_table ADD COLUMN \`array\` Array(('110', 3, toLowCardinality(3), 3, toNullable(3), toLowCardinality(toNullable(3)), 3), UInt8) DEFAULT [1, 2, 3]" 2>&1 | grep -o -F 'Syntax error'

# Collection literals like `(1)` or `[1]` are no longer accepted as data type
# arguments. Real data types do not take tuple/array/map literals as
# parameters; accepting them previously produced an `ASTLiteral` with
# `Field::Tuple` inside the type's argument list which broke AST round-trip
# (see STID 1941-1bfa, fixed by rejecting these at parse time).
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t ADD COLUMN x Array((1), UInt8)" 2>&1 | grep -o -F 'Syntax error'
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t1 (a multiply((NULL), Int8)) ENGINE = Memory" 2>&1 | grep -o -F 'Syntax error'
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t2 (a multiply([NULL], Int8)) ENGINE = Memory" 2>&1 | grep -o -F 'Syntax error'
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t3 (a Nullable(multiply((NULL), Int8))) ENGINE = Memory" 2>&1 | grep -o -F 'Syntax error'

# A function-like name in type position whose uppercase contains "INT" (e.g.
# quantileInterpolatedWeighted) must not be mistaken for a MySQL integer type and
# have its leading (N) group eaten as a display width (which broke the round-trip).
$CLICKHOUSE_FORMAT --oneline --query "CREATE TABLE t (a quantileInterpolatedWeighted(0.8)(a, 1)) ENGINE = Memory" 2>&1 | grep -o -F 'Syntax error'
