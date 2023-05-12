DROP TABLE IF EXISTS bug;
CREATE TABLE IF NOT EXISTS bug(k UInt64, s UInt64) ENGINE = Memory;
insert into bug values(1,21),(1,22),(1,23),(2,21),(2,22),(2,23),(3,21),(3,22),(3,23);

set optimize_min_equality_disjunction_chain_length = 2;

select * from bug;
select * from bug where (k =1 or k=2 or k =3) and (s=21 or s=22 or s=23);
select * from (select * from bug where k=1 or k=2 or k=3) where (s=21 or s=22 or s=23);
select k, (k=1 or k=2 or k=3), s, (s=21), (s=21 or s=22), (s=21 or s=22 or s=23) from bug;
select s, (s=21 or s=22 or s=23) from bug;

set optimize_min_equality_disjunction_chain_length = 3;

select * from bug;
select * from bug where (k =1 or k=2 or k =3) and (s=21 or s=22 or s=23);
select * from (select * from bug where k=1 or k=2 or k=3) where (s=21 or s=22 or s=23);
select k, (k=1 or k=2 or k=3), s, (s=21), (s=21 or s=22), (s=21 or s=22 or s=23) from bug;
select s, (s=21 or s=22 or s=23) from bug;

DROP TABLE bug;
