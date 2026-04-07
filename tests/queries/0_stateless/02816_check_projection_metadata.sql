create table kek (uuid FixedString(16), id int, ns String, dt DateTime64(6), projection null_pk (select * order by ns, 1, 4)) engine=MergeTree order by (id, dt, uuid); -- {serverError ILLEGAL_COLUMN }
-- this query could segfault or throw LOGICAL_ERROR previously, when we did not check projection PK
-- insert into kek select * from generageRandom(10000);
