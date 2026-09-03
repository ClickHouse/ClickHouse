drop table if EXISTS l;
drop table if EXISTS r;

CREATE TABLE l (luid Nullable(Int16), name String)
ENGINE=MergeTree order by luid settings allow_nullable_key=1 as
select * from VALUES ((1231, 'John'),(6666, 'Ksenia'),(Null, '---'));

CREATE TABLE r (ruid Nullable(Int16), name String)
ENGINE=MergeTree order by ruid  settings allow_nullable_key=1 as
select * from VALUES ((1231, 'John'),(1232, 'Johny'));

select 'select 1';
SELECT * FROM l full outer join r on l.luid = r.ruid
where  luid is null 
  and ruid is not null;

select 'select 2';
select * from (
SELECT * FROM l full outer join r on l.luid = r.ruid) 
  where  luid is null 
  and ruid is not null;

select 'select 3';
select * from (
SELECT * FROM l full outer join r on l.luid = r.ruid
limit 100000000) 
  where  luid is null 
  and ruid is not null;

drop table l;
drop table r;

CREATE TABLE l (luid Nullable(Int16), name String) ENGINE=MergeTree order by tuple() as
select * from VALUES ((1231, 'John'),(6666, 'Ksenia'),(Null, '---'));

CREATE TABLE r (ruid Nullable(Int16), name String) ENGINE=MergeTree order by tuple()  as
select * from VALUES ((1231, 'John'),(1232, 'Johny'));

select 'select 4';
SELECT * FROM l full outer join r on l.luid = r.ruid
where  luid is null
  and ruid is not null;

select 'select 5';
select * from (
SELECT * FROM l full outer join r on l.luid = r.ruid)
  where  luid is null
  and ruid is not null;

select 'select 6';
select * from (
SELECT * FROM l full outer join r on l.luid = r.ruid
limit 100000000)
  where  luid is null
  and ruid is not null;

drop table l;
drop table r;
