-- https://github.com/ClickHouse/ClickHouse/issues/36963

DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS b;

create table mt1 (f1 Int32, f2 Int32) engine = MergeTree() order by f1;

create table mt2 as mt1 engine = MergeTree() order by f1;
create table b as mt1 engine = Buffer(currentDatabase(), mt2, 16, 1, 1, 10000, 1000000, 10000000, 100000000);

create table m as mt1 engine = Merge(currentDatabase(), '^(mt1|b)$');

-- insert some data
insert into mt1 values(1, 1), (2, 2);
insert into b   values(3, 3), (4, 4);

OPTIMIZE TABLE b;
OPTIMIZE TABLE mt1;
OPTIMIZE TABLE mt2;

-- do select
select f1, f2
from m
where f1 = 1 and f2 = 1;

DROP TABLE IF EXISTS mt1;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS b;
