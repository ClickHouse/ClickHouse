-- Tags: no-fasttest
set use_statistics = 1;

create table t (a Nullable(Int), b LowCardinality(Nullable(String))) Engine = MergeTree() ORDER BY () settings auto_statistics_types = 'minmax,uniq,tdigest,countmin';
insert into t values (1 , '1'), (2, '2'), (3, '3');
select * from t where a > 1 and b = '1';
