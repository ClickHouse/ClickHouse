#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create a bunch of tables with different partion keys
$CLICKHOUSE_CLIENT -mn --query "
drop table if exists partitioned_single_table;
drop table if exists partitioned_multi_table_date_uint;
drop table if exists partitioned_multi_table_uint_date;
drop table if exists partitioned_multi_table_ipv4_date;
drop table if exists partitioned_multi_table_string_uint_date;
drop table if exists partitioned_multi_table_uint_string_date;
drop table if exists partitioned_multi_table_date_low_cardinality;
drop table if exists partitioned_uint_toYYYYMMDD;
drop table if exists partitioned_uint_toYYYYMM;
drop table if exists partitioned_uint_toYear;
drop table if exists partitioned_uint_toMonth;
drop table if exists partitioned_signed_int;

create table partitioned_single_table (id UInt64, prt_id UInt16) engine = MergeTree order by (id) partition by (prt_id);
create table partitioned_multi_table_date_uint(id UInt64,prt_date Date,prt_id UInt16) engine = MergeTree order by (id) partition by (prt_date, prt_id);
create table partitioned_multi_table_uint_date(id UInt64,prt_date Date,prt_id UInt16) engine = MergeTree order by (id) partition by (prt_id, prt_date);
create table partitioned_multi_table_ipv4_date(id UInt64,prt_date Date,prt_ip_v4 IPv4) engine = MergeTree order by (id) partition by (prt_ip_v4, prt_date);
create table partitioned_multi_table_string_uint_date (id UInt64, prt_date Date, prt_id UInt16, name String) engine = MergeTree order by (id) partition by (name, prt_id, prt_date);
create table partitioned_multi_table_uint_string_date (id UInt64, prt_date Date, prt_id UInt16, name String) engine = MergeTree order by (id) partition by (prt_id, name, prt_date);
create table partitioned_multi_table_date_low_cardinality (id UInt64, prt_date Date, prt_id UInt16, name LowCardinality(String)) engine = MergeTree order by (id) partition by (prt_date, prt_id, name);
-- pin timezone to UTC because it affects the minmax condition expression in the explain output
create table partitioned_uint_toYYYYMMDD (uint UInt16, id UInt64, timestamp DateTime('UTC')) engine = MergeTree order by (uint, id, timestamp) partition by (uint, toYYYYMMDD(timestamp));
create table partitioned_uint_toYYYYMM (uint UInt16, id UInt64, timestamp DateTime('UTC')) engine = MergeTree order by (uint, id, timestamp) partition by (uint, toYYYYMM(timestamp));
create table partitioned_uint_toYear (uint UInt16, id UInt64, timestamp DateTime('UTC')) engine = MergeTree order by (uint, id, timestamp) partition by (uint, toYear(timestamp));
create table partitioned_uint_toMonth (uint UInt16, id UInt64, timestamp DateTime('UTC')) engine = MergeTree order by (uint, id, timestamp) partition by (uint, toMonth(timestamp));
create table partitioned_signed_int (id UInt64, prt_id Int16) engine = MergeTree order by (id) partition by (prt_id);

insert into partitioned_single_table(id, prt_id) select number, number % 100 from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_date_uint(id, prt_id, prt_date) select number, number % 100, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_uint_date(id, prt_id, prt_date) select number, number % 100, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_ipv4_date(id, prt_date, prt_ip_v4) select number, addDays(toDate('2001-01-01'), number % 3), toIPv4(toString(number % 20) || '.' || toString(14 + number % 20) || '.' || toString(35 + number % 120) || '.' || toString(43 + number % 100)) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_date_low_cardinality (id, prt_date, prt_id, name) select number, addDays(toDate('2001-01-01'), number % 3),number % 100, toString(number % 100) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_string_uint_date(id, prt_id, prt_date, name) select number, number % 100, addDays(toDate('2001-01-01'), number % 3), toString(number % 100) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_multi_table_uint_string_date(id, prt_id, prt_date, name) select number, number % 100, addDays(toDate('2001-01-01'), number % 3), toString(number % 100) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_uint_toYYYYMMDD (uint, id, timestamp) select number % 100, number, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_uint_toYYYYMM (uint, id, timestamp) select number % 100, number, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_uint_toYear (uint, id, timestamp) select number % 100, number, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_uint_toMonth (uint, id, timestamp) select number % 100, number, addDays(toDate('2001-01-01'), number % 3) from numbers(1000) settings max_partitions_per_insert_block=65000;
insert into partitioned_signed_int(id, prt_id) select number, (number % 100) - 50 from numbers(1000) settings max_partitions_per_insert_block=65000;
"

# Verify that partition IDs / part prefixes look as expected
function usable_partition_ids() {
    echo "==== partition IDs of $1 should be usable for prefix filtering"
    # partition id consistes of integers separated by '-'
    $CLICKHOUSE_CLIENT --query "select count(distinct _partition_id) from $1;"
    $CLICKHOUSE_CLIENT --query "select distinct splitByChar('_', _part, 1)[1] as partition_id from $1 order by partition_id;"
}

function unusable_partition_ids() {
    echo "==== partition IDs of $1 should not be usable for prefix filtering"
    $CLICKHOUSE_CLIENT --query "select distinct position(splitByChar('_', _part, 1)[1], '-') as partition_id from $1 order by partition_id;"
}

usable_partition_ids partitioned_single_table
usable_partition_ids partitioned_multi_table_date_uint
usable_partition_ids partitioned_multi_table_uint_date
usable_partition_ids partitioned_multi_table_ipv4_date
usable_partition_ids partitioned_uint_toYYYYMMDD
usable_partition_ids partitioned_uint_toYYYYMM
usable_partition_ids partitioned_uint_toYear
usable_partition_ids partitioned_uint_toMonth
unusable_partition_ids partitioned_multi_table_date_low_cardinality
unusable_partition_ids partitioned_multi_table_string_uint_date
unusable_partition_ids partitioned_multi_table_uint_string_date
unusable_partition_ids partitioned_signed_int

# Verify index usage and no overpruning
function check_query() {
    echo
    echo "-- $1"
    for i in 0 1; do
        echo "-- enable_analyzer=$i"
        # Explain query with partition prefix filter enabled
        # Profile events are printed to stderr. We want both the explain output and matching profile events on stdout.
        { $CLICKHOUSE_CLIENT --query "explain indexes=1 $1 SETTINGS allow_partition_prefix_part_filter=1, enable_analyzer=$i" --print-profile-events 2>&1 1>&3 | grep -oP "PartitionPrefixFilter\S+: \d+ \(increment\)"; } 3>&1
        # Check dry-run mode for overpruning
        local overpruned
        overpruned=$($CLICKHOUSE_CLIENT --query "explain indexes=1 $1 SETTINGS allow_partition_prefix_part_filter=1, enable_partition_prefix_part_filter_dry_run=1, enable_analyzer=$i" --print-profile-events 2>&1 | grep -oP "PartitionPrefixFilterOverprunedParts: \K\d+")
        if [[ -n "$overpruned" && "$overpruned" != "0" ]]; then
            echo "ERROR: PartitionPrefixFilterOverprunedParts: $overpruned (increment)"
        fi
        # Compare queried parts to make sure there was no overpruning (this basically double-checks the dry-run mode)
        local query_for_parts
        query_for_parts=$(echo "$1 ORDER BY _part" | sed 's/select \* /select distinct _part /i')
        local parts_with_prefix_filter
        parts_with_prefix_filter=$($CLICKHOUSE_CLIENT --query "$query_for_parts SETTINGS allow_partition_prefix_part_filter=1, enable_analyzer=$i")
        local parts_without_prefix_filter
        parts_without_prefix_filter=$($CLICKHOUSE_CLIENT --query "$query_for_parts SETTINGS allow_partition_prefix_part_filter=0, enable_analyzer=$i")
        if [[ "$parts_with_prefix_filter" != "$parts_without_prefix_filter" ]]; then
            echo "ERROR: Parts mismatch between prefix filter enabled and disabled"
            echo "With prefix filter: $parts_with_prefix_filter"
            echo "Without prefix filter: $parts_without_prefix_filter"
        fi
    done
}

function comment() {
    echo
    echo "$1"
}

comment "==== PartitionPrefix should not prune anything (no conditions or hash-based partition IDs)"
check_query "select * from partitioned_single_table"
check_query "select * from partitioned_multi_table_date_uint"
check_query "select * from partitioned_multi_table_uint_date"
check_query "select * from partitioned_multi_table_string_uint_date"
check_query "select * from partitioned_multi_table_uint_string_date"
check_query "select * from partitioned_multi_table_date_low_cardinality"
check_query "select * from partitioned_multi_table_string_uint_date where name = 'foo'"
check_query "select * from partitioned_multi_table_date_low_cardinality where prt_date = '2001-01-01'"

comment "==== Single-column partition: PartitionPrefix filtering should be used"
check_query "select * from partitioned_single_table where prt_id = 42"
check_query "select * from partitioned_single_table where prt_id = 1"
check_query "select * from partitioned_single_table where prt_id > 50"
check_query "select * from partitioned_single_table where prt_id >= 10 and prt_id <= 20"

comment "==== Lexicographic vs numeric ordering: ranges spanning different digit lengths"
comment "These queries have ranges where min and max have different string lengths."
comment "Lexicographic order differs from numeric order (e.g., '10' < '5' lexicographically)."
check_query "select * from partitioned_single_table where prt_id >= 5 and prt_id <= 15"
check_query "select * from partitioned_single_table where prt_id >= 8 and prt_id <= 12"
check_query "select * from partitioned_single_table where prt_id >= 1 and prt_id <= 100"
check_query "select * from partitioned_single_table where prt_id >= 30 and prt_id <= 55"

comment "==== Multi-column partition: filtering on first column only"
check_query "select * from partitioned_multi_table_date_uint where prt_date = '1990-01-01'"
check_query "select * from partitioned_multi_table_date_uint where prt_date = '2001-01-01'"
check_query "select * from partitioned_multi_table_date_uint where prt_date > '2001-01-01'"
check_query "select * from partitioned_multi_table_date_uint where prt_date < '2001-01-01'"
check_query "select * from partitioned_multi_table_date_uint where prt_date < '2020-01-01'"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42"
check_query "select * from partitioned_multi_table_uint_date where prt_id >= 10 and prt_id <= 20"

comment "==== Multi-column partition: filtering on second column only (should not prune anything)"
check_query "select * from partitioned_multi_table_uint_date where prt_date >= '2001-01-01'"

comment "==== Multi-column partition: filtering on first AND second column"
check_query "select * from partitioned_multi_table_date_uint where prt_date = '2001-01-01' and prt_id = 42"
check_query "select * from partitioned_multi_table_date_uint where prt_date = '2001-01-01' and prt_id >= 10 and prt_id <= 20"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 and prt_date = '2001-01-01'"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 and prt_date >= '2001-01-01'"
check_query "select * from partitioned_multi_table_date_uint where prt_date >= '2001-01-01' and prt_id = 42"
check_query "select * from partitioned_multi_table_uint_date where prt_id >= 10 and prt_id <= 20 and prt_date = '2001-01-01'"

comment "==== IPv4 partition column"
check_query "select * from partitioned_multi_table_ipv4_date where prt_ip_v4 = toIPv4('1.15.36.44')"
check_query "select * from partitioned_multi_table_ipv4_date where prt_ip_v4 = toIPv4('1.15.36.44') and prt_date = '2001-01-01'"

comment "==== OR conditions: should compute union of ranges"
comment "-- OR on same column produces a range covering both values"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 or prt_id = 43"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 10 or prt_id = 50"
comment "-- OR on different columns: must match everything"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 10 or prt_date = '2001-01-01'"
comment "-- complex example"
check_query "select * from partitioned_multi_table_uint_date where (prt_date = '2001-01-01' and (prt_id != 20 and (prt_id = 10 or prt_id = 20))) or (prt_date <= '2002-01-01' and prt_id = 50)"

comment "==== NOT / negation: conservatively matches everything (no prefix filtering)"
check_query "select * from partitioned_multi_table_uint_date where not prt_id = 42"
check_query "select * from partitioned_multi_table_uint_date where prt_id != 42"

comment "==== IN set / NOT IN set: treated as match-everything (no prefix filtering)"
check_query "select * from partitioned_multi_table_uint_date where prt_id in (10, 20, 30)"
check_query "select * from partitioned_multi_table_uint_date where prt_id not in (10, 20, 30)"

comment "==== NULL checks: treated as match-everything (no prefix filtering)"
check_query "select * from partitioned_multi_table_uint_date where prt_id is not null"

comment "==== Mixed conditions: AND with match-everything still uses the usable range"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 and prt_date in ('2001-01-01', '2001-01-02')"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 and prt_date is not null"

comment "==== Functions applied to partition key: only filter if possible"
check_query "select * from partitioned_multi_table_date_uint where toYear(prt_date) = 2001"
check_query "select * from partitioned_multi_table_uint_date where prt_id + 1 = 43"

comment "==== Complex filtering expressions"
comment "Deeply nested AND/OR - 4 levels deep"
check_query "select * from partitioned_multi_table_uint_date where ((prt_id = 10 or prt_id = 20) and (prt_id = 15 or prt_id = 20)) or ((prt_id = 30 or prt_id = 40) and (prt_id = 35 or prt_id = 40))"
comment "Nested ORs inside ANDs inside ORs"
check_query "select * from partitioned_multi_table_uint_date where (((prt_id = 10 or prt_id = 11) and prt_date = '2001-01-01') or ((prt_id = 20 or prt_id = 21) and prt_date = '2001-01-02')) or prt_id = 50"
comment "Complex: multiple NOTs nested"
check_query "select * from partitioned_multi_table_uint_date where prt_id = 42 and not (prt_date = '2001-01-01' or prt_date = '2001-01-02')"
check_query "select * from partitioned_multi_table_uint_date where not (not (prt_id = 42))"
comment "Deeply nested ranges with AND/OR"
check_query "select * from partitioned_multi_table_uint_date where (((prt_id >= 10 and prt_id <= 20) or (prt_id >= 30 and prt_id <= 40)) and ((prt_date >= '2001-01-01' and prt_date <= '2001-01-02') or prt_date = '2001-01-03'))"
comment "Mix of point, range, and complex conditions - 5 levels"
check_query "select * from partitioned_multi_table_uint_date where ((prt_id = 10 and (prt_date = '2001-01-01' or (prt_date >= '2001-01-02' and prt_date <= '2001-01-03'))) or (prt_id = 20 and prt_date = '2001-01-01')) and prt_id < 50"
comment "Alternating AND/OR at each level"
check_query "select * from partitioned_multi_table_uint_date where (prt_id = 10 or (prt_id = 20 and (prt_id = 20 or (prt_id = 30 and prt_date = '2001-01-01'))))"
comment "Complex with functions mixed in (should be conservative on function parts)"
check_query "select * from partitioned_multi_table_uint_date where (prt_id = 42 or prt_id = 43) and (prt_id + 0 = prt_id)"
comment "Triple OR of complex ANDs"
check_query "select * from partitioned_multi_table_uint_date where (prt_id = 10 and prt_date = '2001-01-01') or (prt_id = 20 and prt_date = '2001-01-02') or (prt_id = 30 and prt_date = '2001-01-03')"
comment "Nested with IN sets (match-everything parts)"
check_query "select * from partitioned_multi_table_uint_date where ((prt_id = 42 or prt_id = 43) and prt_date in ('2001-01-01', '2001-01-02')) or prt_id = 50"
comment "Very deep: 6 levels of nesting"
check_query "select * from partitioned_multi_table_uint_date where (((((prt_id = 10 or prt_id = 11) and prt_date >= '2001-01-01') or prt_id = 20) and prt_date <= '2001-01-03') or prt_id = 30) and prt_id < 100"
comment "Complex contradictions nested"
check_query "select * from partitioned_multi_table_uint_date where (prt_id = 10 and prt_id = 20) or (prt_id = 30 and prt_date = '2001-01-01')"
comment "Nested with NULL checks"
check_query "select * from partitioned_multi_table_uint_date where ((prt_id = 42 and prt_date is not null) or (prt_id = 43 and prt_date is not null)) and prt_id is not null"
comment "Edge cases"
check_query "select * from partitioned_multi_table_uint_date where prt_id > 100"
check_query "select * from partitioned_multi_table_uint_date where prt_id >= 0"

comment "==== transformed timestamp partitioning"
check_query "select * from partitioned_uint_toYYYYMMDD where uint = 42"
check_query "select * from partitioned_uint_toYYYYMMDD where uint = 42 and timestamp >= '2001-01-01'"
check_query "select * from partitioned_uint_toYYYYMM where uint = 42"
check_query "select * from partitioned_uint_toYYYYMM where uint = 42 and timestamp >= '2001-01-01'"
check_query "select * from partitioned_uint_toYYYYMM where uint = 42 and timestamp >= '2001-06-06'"
check_query "select * from partitioned_uint_toYear where uint = 42"
check_query "select * from partitioned_uint_toYear where uint = 42 and timestamp >= '2001-01-01'"
check_query "select * from partitioned_uint_toYear where uint = 42 and timestamp >= '2001-06-06'"
check_query "select * from partitioned_uint_toMonth where uint = 42"
check_query "select * from partitioned_uint_toMonth where uint = 42 and timestamp >= '2001-01-01'" 

comment "==== Signed integer partition key (should not use prefix filtering)"
check_query "select * from partitioned_signed_int where prt_id = 10"
check_query "select * from partitioned_signed_int where prt_id >= -10 and prt_id <= 10"
check_query "select * from partitioned_signed_int where prt_id >= 0"

$CLICKHOUSE_CLIENT -mn --query "
drop table if exists partitioned_single_table;
drop table if exists partitioned_multi_table_date_uint;
drop table if exists partitioned_multi_table_uint_date;
drop table if exists partitioned_multi_table_ipv4_date;
drop table if exists partitioned_multi_table_string_uint_date;
drop table if exists partitioned_multi_table_uint_string_date;
drop table if exists partitioned_multi_table_date_low_cardinality;
drop table if exists partitioned_uint_toYYYYMMDD;
drop table if exists partitioned_uint_toYYYYMM;
drop table if exists partitioned_uint_toYear;
drop table if exists partitioned_uint_toMonth;
drop table if exists partitioned_signed_int;
"
