-- Tags: no-fasttest
-- virtual hosted style
create table s3_03364 (id UInt32) engine=S3('http://{_partition_id}.s3.region.amazonaws.com/key'); -- {serverError BAD_ARGUMENTS}
create table s3_03364 (id UInt32) engine=S3('http://{_partition_id}something.s3.region.amazonaws.com/key'); -- {serverError BAD_ARGUMENTS}

select * from s3('http://{_partition_id}.s3.region.amazonaws.com/key', 'Parquet'); -- {serverError BAD_ARGUMENTS}
select * from s3('http://{_partition_id}something.s3.region.amazonaws.com/key', 'Parquet'); -- {serverError BAD_ARGUMENTS}

insert into table function s3('http://{_partition_id}.s3.region.amazonaws.com/key', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}
insert into table function s3('http://{_partition_id}something.s3.region.amazonaws.com/key', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}

-- path style
create table s3_03364 (id UInt32) engine=S3('http://s3.region.amazonaws.com/{_partition_id}'); -- {serverError BAD_ARGUMENTS}
create table s3_03364 (id UInt32) engine=S3('http://s3.region.amazonaws.com/{_partition_id}/key'); -- {serverError BAD_ARGUMENTS}

select * from s3('http://s3.region.amazonaws.com/{_partition_id}', 'Parquet'); -- {serverError BAD_ARGUMENTS}
select * from s3('http://s3.region.amazonaws.com/{_partition_id}/key', 'Parquet'); -- {serverError BAD_ARGUMENTS}

insert into table function s3('http://s3.region.amazonaws.com/{_partition_id}', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}
insert into table function s3('http://s3.region.amazonaws.com/{_partition_id}/key', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}

-- aws private link style
create table s3_03364 (id UInt32) engine=S3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}'); -- {serverError BAD_ARGUMENTS}
create table s3_03364 (id UInt32) engine=S3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}/key'); -- {serverError BAD_ARGUMENTS}

select * from s3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}', 'Parquet'); -- {serverError BAD_ARGUMENTS}
select * from s3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}/key', 'Parquet'); -- {serverError BAD_ARGUMENTS}

insert into table function s3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}
insert into table function s3('http://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/{_partition_id}/key', 'NOSIGN', 'Parquet') select * from numbers(5); -- {serverError BAD_ARGUMENTS}
