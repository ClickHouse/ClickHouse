-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A nullable Paimon ARRAY/MAP column must not be wrapped in Nullable
-- (ClickHouse forbids Nullable(Array) and Nullable(Map), which made the whole
-- table unreadable); NULL composite values are read as empty ones.
-- https://github.com/ClickHouse/ClickHouse/issues/113337

desc paimonS3(s3_conn, filename='paimon_nullable_composites');
select '===';
select id, arr, m from paimonS3(s3_conn, filename='paimon_nullable_composites') order by id;
