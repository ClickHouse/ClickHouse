drop table if exists data_02230_ttl;
drop table if exists null_02230_ttl;
create table data_02230_ttl (date Date, key Int) Engine=MergeTree() order by key TTL date + 14;
show create data_02230_ttl format TSVRaw;
create table null_02230_ttl engine=Null() as data_02230_ttl;
show create null_02230_ttl format TSVRaw;
drop table data_02230_ttl;
drop table null_02230_ttl;

drop table if exists data_02230_column_ttl;
drop table if exists null_02230_column_ttl;
create table data_02230_column_ttl (date Date, value Int TTL date + 7, key Int) Engine=MergeTree() order by key TTL date + 14;
show create data_02230_column_ttl format TSVRaw;
create table null_02230_column_ttl engine=Null() as data_02230_column_ttl;
-- check that order of columns is the same
show create null_02230_column_ttl format TSVRaw;
drop table data_02230_column_ttl;
drop table null_02230_column_ttl;
