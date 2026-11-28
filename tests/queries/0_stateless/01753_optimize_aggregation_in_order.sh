#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=trace

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --optimize_aggregation_in_order=1 -m -q "
drop table if exists data_01753;
create table data_01753 (key Int) engine=MergeTree() order by key as select * from numbers(8);
select * from data_01753 group by key settings max_block_size=1;
select * from data_01753 group by key settings max_block_size=1;
drop table data_01753;
" |& grep -F -c 'AggregatingInOrderTransform: Aggregated. 8 to 8 rows'
