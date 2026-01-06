#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CLICKHOUSE_TMP"


$CLICKHOUSE_CLIENT --query="insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}.jer', 'CSV') select (tuple(tuple(''), ''), tuple(tuple('a'), 'a')) union all select (tuple(tuple('a'), 'a'), tuple(tuple(''), ''))"

$CLICKHOUSE_CLIENT --query="create table hmmm (a Tuple(b Tuple(c String), d String), e Tuple(f Tuple(g String), h String), t  DateTime DEFAULT now()) engine = MergeTree order by (e.h, a.d) ttl t + toIntervalDay(2) settings min_bytes_for_wide_part=1, ratio_of_defaults_for_sparse_serialization=0.01;"
$CLICKHOUSE_CLIENT --query="insert into hmmm (a, e) values (tuple(tuple(''), ''), tuple(tuple('a'), 'a')), (tuple(tuple('a'), 'a'), tuple(tuple(''), ''));"
$CLICKHOUSE_CLIENT --query="insert into hmmm (a, e) select * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.jer', 'CSV')"

rm -f 03783_nested_sparse_serialization_bug.jer
