#!/usr/bin/env bash
# Tags: no-object-storage, no-parallel, no-fasttest

# no-object-storage: s3 has 20 more threads
# no-parallel: it checks the number of threads, which can be lowered in presence of other queries

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


cat <<EOF | $CLICKHOUSE_CLIENT -n $SETTINGS
drop table if exists testX;
drop table if exists testXA;
drop table if exists testXB;
drop table if exists testXC;

create table testX (A Int64) engine=MergeTree order by tuple();

create materialized view testXA engine=MergeTree order by tuple() as select sleep(0.1) from testX;
create materialized view testXB engine=MergeTree order by tuple() as select sleep(0.2), throwIf(A=1) from testX;
create materialized view testXC engine=MergeTree order by tuple() as select sleep(0.1) from testX;
EOF

for max_threads in 1 7; do
    for max_insert_threads in 1 4; do
        echo "max_threads: $max_threads max_insert_threads: $max_insert_threads"

        QUERY_ID="03652_query_id_$RANDOM"
        SETTINGS="--query_id=$QUERY_ID "
        SETTINGS="$SETTINGS --max_threads=$max_threads "
        SETTINGS="$SETTINGS --max_insert_threads=$max_insert_threads "

        SETTINGS="$SETTINGS --max_block_size=10 "
        SETTINGS="$SETTINGS --min_insert_block_size_rows=10 "
        SETTINGS="$SETTINGS --materialized_views_ignore_errors=1 "
        SETTINGS="$SETTINGS --parallel_view_processing=1 "
        SETTINGS="$SETTINGS --log_queries=1 "
        SETTINGS="$SETTINGS --send_logs_level=error "

        $CLICKHOUSE_CLIENT -q 'select * from numbers(200) format TSV' | $CLICKHOUSE_CLIENT $SETTINGS -q 'insert into testX FORMAT TSV'

        $CLICKHOUSE_CLIENT -q 'system flush logs system.query_log;'

        cat <<EOF | $CLICKHOUSE_CLIENT
select
    if(peak_threads_usage >= 6, 7, peak_threads_usage),
from system.query_log where
    current_database = currentDatabase() and
    type != 'QueryStart' and
    query_id = '$QUERY_ID'
order by ALL;
EOF

    done
done
