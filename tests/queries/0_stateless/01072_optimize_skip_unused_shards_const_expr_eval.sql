drop table if exists data_01072;
drop table if exists dist_01072;

set optimize_skip_unused_shards=1;
set force_optimize_skip_unused_shards=1;

create table data_01072 (key Int, value Int, str String) Engine=Null();
create table dist_01072 (key Int, value Int, str String) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);

select * from dist_01072 where key=0 and length(str)=0;
select * from dist_01072 where key=0 and str='';
select * from dist_01072 where xxHash64(0)==xxHash64(0) and key=0;
select * from dist_01072 where key=toInt32OrZero(toString(xxHash64(0)));
select * from dist_01072 where key=toInt32(xxHash32(0));
select * from dist_01072 where key=toInt32(toInt32(xxHash32(0)));
select * from dist_01072 where key=toInt32(toInt32(toInt32(xxHash32(0))));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=toInt32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=toInt32(value) settings force_optimize_skip_unused_shards=0;

drop table dist_01072;
create table dist_01072 (key Int, value Nullable(Int), str String) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=toInt32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=toInt32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=toInt32(value) settings force_optimize_skip_unused_shards=0;

set allow_suspicious_low_cardinality_types=1;

drop table dist_01072;
create table dist_01072 (key Int, value LowCardinality(Int), str String) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=toInt32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=toInt32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=toInt32(value) settings force_optimize_skip_unused_shards=0;

drop table dist_01072;
create table dist_01072 (key Int, value LowCardinality(Nullable(Int)), str String) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key%2);
select * from dist_01072 where key=toInt32(xxHash32(0));
select * from dist_01072 where key=value; -- { serverError 507; }
select * from dist_01072 where key=toInt32(value); -- { serverError 507; }
select * from dist_01072 where key=value settings force_optimize_skip_unused_shards=0;
select * from dist_01072 where key=toInt32(value) settings force_optimize_skip_unused_shards=0;

-- check virtual columns
drop table data_01072;
drop table dist_01072;
create table data_01072 (key Int) Engine=MergeTree() ORDER BY key;
create table dist_01072 (key Int) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01072, key);
select * from dist_01072 where key=0 and _part='0' settings force_optimize_skip_unused_shards=2;

drop table data_01072;
drop table dist_01072;
