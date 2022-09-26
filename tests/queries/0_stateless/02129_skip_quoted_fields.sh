#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_02129"
$CLICKHOUSE_CLIENT -q "create table test_02129 (x UInt64, y UInt64) engine=Memory()"

QUERY="insert into test_02129 format CustomSeparatedWithNames settings input_format_skip_unknown_fields=1, format_custom_escaping_rule='Quoted'"

# Skip string
echo -e "'x'\t'trash'\t'y'\n1\t'Some string'\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

# Skip number
echo -e "'x'\t'trash'\t'y'\n2\t42\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n3\t4242.4242\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n4\t-42\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n5\t+42\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n6\t-4242.424242\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n7\t+4242.424242\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n8\tnan\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n9\tinf\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n10\t+nan\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n11\t+inf\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n12\t-nan\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n13\t-inf\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n14\t44444444444444444444444444.444444444444444444444444\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n15\t30e30\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n16\t-30e-30\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

# Skip NULL
echo -e "'x'\t'trash'\t'y'\n17\tNULL\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

# Skip an array
echo -e "'x'\t'trash'\t'y'\n18\t[1,2,3,4]\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n19\t['some string ]][[][][]', 'one more string (){}][[{[[[[[[']\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n20\t[[(1,2), (3,4)], [(5,6), (7,8)]]\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

# Skip a tuple
echo -e "'x'\t'trash'\t'y'\n21\t(1,2,3,4)\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n22\t('some string ()))))(()(())', 'one more string (){}][[{[)))))')\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n23\t(([1,2], (3,4)), ([5,6], (7,8)))\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

# Skip a map
echo -e "'x'\t'trash'\t'y'\n24\t{1:2,2:3,3:4,4:5}\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n25\t{'some string }}}}}}{{{{':123, 'one more string (){}][[{[{{{{{':123}\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"
echo -e "'x'\t'trash'\t'y'\n26\t{'key':{1:(1,2), 2:(3,4)}, 'foo':{1:(5,6), 2:(7,8)}}\t42" | $CLICKHOUSE_CLIENT -q "$QUERY"

$CLICKHOUSE_CLIENT -q "select * from test_02129 order by x"
$CLICKHOUSE_CLIENT -q "drop table test_02129"

