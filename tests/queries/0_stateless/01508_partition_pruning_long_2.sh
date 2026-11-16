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
DROP TABLE IF EXISTS tDD;
DROP TABLE IF EXISTS sDD;
DROP TABLE IF EXISTS xMM;

CREATE TABLE tDD(d DateTime('Asia/Istanbul'),a Int) ENGINE = MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY tuple() SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES tDD;
insert into tDD select toDateTime(toDate('2020-09-23'), 'Asia/Istanbul'), number from numbers(10000) UNION ALL select toDateTime(toDateTime('2020-09-23 11:00:00', 'Asia/Istanbul')), number from numbers(10000) UNION ALL select toDateTime(toDate('2020-09-24'), 'Asia/Istanbul'), number from numbers(10000) UNION ALL select toDateTime(toDate('2020-09-25'), 'Asia/Istanbul'), number from numbers(10000) UNION ALL select toDateTime(toDate('2020-08-15'), 'Asia/Istanbul'), number from numbers(10000);

CREATE TABLE sDD(d UInt64,a Int) ENGINE = MergeTree PARTITION BY toYYYYMM(toDate(intDiv(d,1000), 'Asia/Istanbul')) ORDER BY tuple() SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES sDD;
insert into sDD select (1597536000+number*60)*1000, number from numbers(5000);
insert into sDD select (1597536000+number*60)*1000, number from numbers(5000);
insert into sDD select (1598918400+number*60)*1000, number from numbers(5000);
insert into sDD select (1598918400+number*60)*1000, number from numbers(5000);
insert into sDD select (1601510400+number*60)*1000, number from numbers(5000);
insert into sDD select (1602720000+number*60)*1000, number from numbers(5000);

CREATE TABLE xMM(d DateTime('Asia/Istanbul'),a Int64, f Int64) ENGINE = MergeTree PARTITION BY (toYYYYMM(d), a) ORDER BY tuple() SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES xMM;
INSERT INTO xMM SELECT toDateTime('2020-08-16 00:00:00', 'Asia/Istanbul') + number*60, 1, number FROM numbers(5000);
INSERT INTO xMM SELECT toDateTime('2020-08-16 00:00:00', 'Asia/Istanbul') + number*60, 2, number FROM numbers(5000);
INSERT INTO xMM SELECT toDateTime('2020-09-01 00:00:00', 'Asia/Istanbul') + number*60, 3, number FROM numbers(5000);
INSERT INTO xMM SELECT toDateTime('2020-09-01 00:00:00', 'Asia/Istanbul') + number*60, 2, number FROM numbers(5000);
INSERT INTO xMM SELECT toDateTime('2020-10-01 00:00:00', 'Asia/Istanbul') + number*60, 1, number FROM numbers(5000);
INSERT INTO xMM SELECT toDateTime('2020-10-15 00:00:00', 'Asia/Istanbul') + number*60, 1, number FROM numbers(5000);


SELECT '--------- tDD ----------------------------';
SYSTEM START MERGES tDD;
OPTIMIZE TABLE tDD FINAL;

select uniqExact(_part), count() from tDD where toDate(d)=toDate('2020-09-24');
select uniqExact(_part), count() FROM tDD WHERE toDate(d) = toDate('2020-09-24');
select uniqExact(_part), count() FROM tDD WHERE toDate(d) = '2020-09-24';
select uniqExact(_part), count() FROM tDD WHERE toDate(d) >= '2020-09-23' and toDate(d) <= '2020-09-26';
select uniqExact(_part), count() FROM tDD WHERE toYYYYMMDD(d) >= 20200923 and toDate(d) <= '2020-09-26';


SELECT '--------- sDD ----------------------------';
select uniqExact(_part), count() from sDD;
select uniqExact(_part), count() from sDD where toYYYYMM(toDateTime(intDiv(d,1000),'UTC')-1)+1 = 202010;
select uniqExact(_part), count() from sDD where toYYYYMM(toDateTime(intDiv(d,1000),'UTC')-1) = 202010;
select uniqExact(_part), count() from sDD where toYYYYMM(toDateTime(intDiv(d,1000),'UTC')-1) = 202110;
select uniqExact(_part), count() from sDD where toYYYYMM(toDateTime(intDiv(d,1000),'UTC'))+1 > 202009 and toStartOfDay(toDateTime(intDiv(d,1000),'UTC')) < toDateTime('2020-10-02 00:00:00','UTC');
select uniqExact(_part), count() from sDD where toYYYYMM(toDateTime(intDiv(d,1000),'UTC'))+1 > 202009 and toDateTime(intDiv(d,1000),'UTC') < toDateTime('2020-10-01 00:00:00','UTC');
select uniqExact(_part), count() from sDD where d >= 1598918400000;
select uniqExact(_part), count() from sDD where d >= 1598918400000 and toYYYYMM(toDateTime(intDiv(d,1000),'UTC')-1) < 202010;


SELECT '--------- xMM ----------------------------';
select uniqExact(_part), count() from xMM where toStartOfDay(d) >= '2020-10-01 00:00:00';
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d <= '2020-10-01 00:00:00';
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d < '2020-10-01 00:00:00';
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d <= '2020-10-01 00:00:00' and a=1;
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d <= '2020-10-01 00:00:00' and a<>3;
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d < '2020-10-01 00:00:00' and a<>3;
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d < '2020-11-01 00:00:00' and a = 1;
select uniqExact(_part), count() from xMM where a = 1;
select uniqExact(_part), count() from xMM where a = 66;
select uniqExact(_part), count() from xMM where a <> 66;
select uniqExact(_part), count() from xMM where a = 2;

SYSTEM START MERGES xMM;
optimize table xMM final;

select uniqExact(_part), count() from xMM where a = 1;
select uniqExact(_part), count() from xMM where toStartOfDay(d) >= '2020-10-01 00:00:00';
select uniqExact(_part), count() from xMM where a <> 66;
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d <= '2020-10-01 00:00:00' and a<>3;
select uniqExact(_part), count() from xMM where d >= '2020-09-01 00:00:00' and d < '2020-10-01 00:00:00' and a<>3;

DROP TABLE tDD;
DROP TABLE sDD;
DROP TABLE xMM;
"
