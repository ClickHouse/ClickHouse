drop table if EXISTS l;
drop table if EXISTS r;

CREATE TABLE l (luid Nullable(Int16), name String) ENGINE=MergeTree order by luid settings allow_nullable_key=1;
CREATE TABLE r (ruid Nullable(Int16), name String) ENGINE=MergeTree order by ruid  settings allow_nullable_key=1;

INSERT INTO l VALUES (1231, 'John');
INSERT INTO l VALUES (6666, 'Ksenia');
INSERT INTO l VALUES (Null, '---');

INSERT INTO r VALUES (1231, 'John');
INSERT INTO r VALUES (1232, 'Johny');

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
