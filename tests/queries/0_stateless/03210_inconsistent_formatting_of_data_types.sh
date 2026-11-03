#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Ensure that these (possibly incorrect) queries can at least be parsed back after formatting.
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE columns_with_multiple_streams MODIFY COLUMN field1 Nullable(tupleElement(x, 2), UInt8)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t_update_empty_nested ADD COLUMN \`nested.arr2\` Array(tuple('- ON NULL -', toLowCardinality(11), 11, 11, toLowCardinality(11), 11), UInt64)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t ADD COLUMN x Array((1), UInt8)" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE enum_alter_issue (MODIFY COLUMN a Enum8(equals('one', timeSlots(timeSlots(arrayEnumerateDense(tuple('0.2147483646', toLowCardinality(toUInt128(12))), NULL), 4, 12.34, materialize(73), 2)), 1)))" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t_sparse_mutations_3 MODIFY COLUMN s Tuple(Nullable(tupleElement(s, 1), UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(UInt64), Nullable(String))" | $CLICKHOUSE_FORMAT --oneline

# These invalid queries don't parse and this is normal.
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE alter_compression_codec1 MODIFY COLUMN alter_column CODEC((2 + ignore(1, toUInt128(materialize(2)), 2 + toNullable(toNullable(3))), 3), NONE)" 2>&1 | grep -o -F 'Syntax error'
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE test_table ADD COLUMN \`array\` Array(('110', 3, toLowCardinality(3), 3, toNullable(3), toLowCardinality(toNullable(3)), 3), UInt8) DEFAULT [1, 2, 3]" 2>&1 | grep -o -F 'Syntax error'
