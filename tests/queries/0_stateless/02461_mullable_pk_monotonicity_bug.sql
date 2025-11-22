create table tab (x Nullable(UInt8)) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);
set allow_suspicious_low_cardinality_types=1;
set max_rows_to_read = 2;

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::Nullable(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(Nullable(UInt8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::Nullable(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(Nullable(UInt8)), x) <= 2 order by x;

drop table tab;
set max_rows_to_read = 100;
create table tab (x LowCardinality(UInt8)) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::Nullable(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(Nullable(UInt8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::Nullable(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(Nullable(UInt8)), x) <= 2 order by x;

drop table tab;
set max_rows_to_read = 100;
create table tab (x UInt128) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::Nullable(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(UInt8)) <= 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(Nullable(UInt8))) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::Nullable(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(UInt8), x) <= 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(Nullable(UInt8)), x) <= 2 order by x;

set max_rows_to_read = 100;
SELECT x + 1 FROM tab WHERE (x + 1::LowCardinality(UInt8)) <= -9223372036854775808 order by x;

drop table tab;
create table tab (x DateTime) engine = MergeTree order by x settings allow_nullable_key = 1, index_granularity = 2;
insert into tab select toDateTime('2022-02-02') + number from numbers(4);

set max_rows_to_read = 2;
SELECT x + 1 FROM tab where plus(x, 1) <= toDateTime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::Nullable(UInt8)) <= toDateTime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(UInt8)) <= toDateTime('2022-02-02') + 2 order by x;
SELECT x + 1 FROM tab where plus(x, 1::LowCardinality(Nullable(UInt8))) <= toDateTime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1, x) <= toDateTime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::Nullable(UInt8), x) <= toDateTime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(UInt8), x) <= toDateTime('2022-02-02') + 2 order by x;
SELECT 1 + x FROM tab where plus(1::LowCardinality(Nullable(UInt8)), x) <= toDateTime('2022-02-02') + 2 order by x;

SELECT x + 1 FROM tab WHERE (x + CAST('1', 'Nullable(UInt8)')) <= -2147483647 ORDER BY x ASC NULLS FIRST;
