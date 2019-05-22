DROP TABLE IF EXISTS ANIMAL;

CREATE TABLE ANIMAL ( ANIMAL Nullable(String) ) engine = TinyLog;
INSERT INTO ANIMAL (ANIMAL) VALUES ('CAT'), ('FISH'), ('DOG'), ('HORSE'), ('BIRD');

select * from (
select x.b x, count(distinct x.c) ANIMAL
from (
select a.ANIMAL a, 'CAT' b, c.ANIMAL c, d.ANIMAL d
from ANIMAL a join ANIMAL b on a.ANIMAL = b.ANIMAL
        left outer  join ANIMAL c on (b.ANIMAL = c.ANIMAL)
        right outer join (select * from ANIMAL union all select * from ANIMAL
                          union all select * from ANIMAL)  d on (a.ANIMAL = d.ANIMAL)
where d.ANIMAL <> 'CAT' and c.ANIMAL <>'DOG' and b.ANIMAL <> 'FISH') as x
where x.b >= 'CAT'
group by x.b
having ANIMAL >= 0) ANIMAL
where ANIMAL.ANIMAL >= 0;

DROP TABLE ANIMAL;
