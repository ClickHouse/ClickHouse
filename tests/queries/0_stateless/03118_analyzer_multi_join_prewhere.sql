-- https://github.com/ClickHouse/ClickHouse/issues/4596
SET enable_analyzer=1;

CREATE TABLE a1 ( ANIMAL Nullable(String) ) engine = MergeTree order by tuple();
insert into a1 values('CROCO');

select count()
     from a1 a
      join a1 b on (a.ANIMAL = b.ANIMAL)
      join a1 c on (c.ANIMAL = b.ANIMAL)
where a.ANIMAL = 'CROCO';

select count()
     from a1 a
     join a1 b on (a.ANIMAL = b.ANIMAL)
     join a1 c on (c.ANIMAL = b.ANIMAL)
prewhere a.ANIMAL = 'CROCO';
