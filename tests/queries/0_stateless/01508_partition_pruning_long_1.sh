#!/usr/bin/env bash
# Tags: long, no-polymorphic-parts, no-random-settings, no-random-merge-tree-settings, no-debug

# Description of test result:
# Test the correctness of the partition pruning
#
# Script executes queries from a file 01508_partition_pruning_long.queries (1 line = 1 query)
# Queries are started with 'select' (but NOT with 'SELECT') are executed with log_level=debug

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


while IFS= read -r sql
do
  [ -z "$sql" ] && continue
  if [[ "$sql" == select* ]] ;
  then
    echo "$sql"
    ${CLICKHOUSE_CLIENT} --query "$sql"
    CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')
    ${CLICKHOUSE_CLIENT} --query "$sql" 2>&1 | grep -oh "Selected .* parts by partition key, *. parts by primary key, .* marks by primary key, .* marks to read from .* ranges.*$"
    CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=debug/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/g')
    echo ""
  else
    ${CLICKHOUSE_CLIENT} --query "$sql"
  fi
done <<< "
DROP TABLE IF EXISTS tMM;

CREATE TABLE tMM(d DateTime('Asia/Istanbul'), a Int64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY tuple() SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES tMM;
INSERT INTO tMM SELECT toDateTime('2020-08-16 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);
INSERT INTO tMM SELECT toDateTime('2020-08-16 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);
INSERT INTO tMM SELECT toDateTime('2020-09-01 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);
INSERT INTO tMM SELECT toDateTime('2020-09-01 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);
INSERT INTO tMM SELECT toDateTime('2020-10-01 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);
INSERT INTO tMM SELECT toDateTime('2020-10-15 00:00:00', 'Asia/Istanbul') + number*60, number FROM numbers(5000);

SELECT '--------- tMM ----------------------------';
select uniqExact(_part), count() from tMM where toDate(d)=toDate('2020-09-15');
select uniqExact(_part), count() from tMM where toDate(d)=toDate('2020-09-01');
select uniqExact(_part), count() from tMM where toDate(d)=toDate('2020-10-15');
select uniqExact(_part), count() from tMM where toDate(d)='2020-09-15';
select uniqExact(_part), count() from tMM where toYYYYMM(d)=202009;
select uniqExact(_part), count() from tMM where toYYYYMMDD(d)=20200816;
select uniqExact(_part), count() from tMM where toYYYYMMDD(d)=20201015;
select uniqExact(_part), count() from tMM where toDate(d)='2020-10-15';
select uniqExact(_part), count() from tMM where d >= '2020-09-01 00:00:00' and d<'2020-10-15 00:00:00';
select uniqExact(_part), count() from tMM where d >= '2020-01-16 00:00:00' and d < toDateTime('2021-08-17 00:00:00', 'Asia/Istanbul');
select uniqExact(_part), count() from tMM where d >= '2020-09-16 00:00:00' and d < toDateTime('2020-10-01 00:00:00', 'Asia/Istanbul');
select uniqExact(_part), count() from tMM where d >= '2020-09-12 00:00:00' and d < '2020-10-16 00:00:00';
select uniqExact(_part), count() from tMM where toStartOfDay(d) >= '2020-09-12 00:00:00';
select uniqExact(_part), count() from tMM where toStartOfDay(d) = '2020-09-01 00:00:00';
select uniqExact(_part), count() from tMM where toStartOfDay(d) = '2020-10-01 00:00:00';
select uniqExact(_part), count() from tMM where toStartOfDay(d) >= '2020-09-15 00:00:00' and d < '2020-10-16 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d) between 202009 and 202010;
select uniqExact(_part), count() from tMM where toYYYYMM(d) between 202009 and 202009;
select uniqExact(_part), count() from tMM where toYYYYMM(d) between 202009 and 202010 and toStartOfDay(d) = '2020-10-01 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d) >= 202009 and toStartOfDay(d) < '2020-10-02 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d) > 202009 and toStartOfDay(d) < '2020-10-02 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d)+1 > 202009 and toStartOfDay(d) < '2020-10-02 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d)+1 > 202010 and toStartOfDay(d) < '2020-10-02 00:00:00';
select uniqExact(_part), count() from tMM where toYYYYMM(d)+1 > 202010;
select uniqExact(_part), count() from tMM where toYYYYMM(d-1)+1 = 202010;
select uniqExact(_part), count() from tMM where toStartOfMonth(d) >= '2020-09-15';
select uniqExact(_part), count() from tMM where toStartOfMonth(d) >= '2020-09-01';
select uniqExact(_part), count() from tMM where toStartOfMonth(d) >= '2020-09-01' and toStartOfMonth(d) < '2020-10-01';

SYSTEM START MERGES tMM;
OPTIMIZE TABLE tMM FINAL;

select uniqExact(_part), count() from tMM where toYYYYMM(d-1)+1 = 202010;
select uniqExact(_part), count() from tMM where toYYYYMM(d)+1 > 202010;
select uniqExact(_part), count() from tMM where toYYYYMM(d) between 202009 and 202010;

DROP TABLE tMM;
"
