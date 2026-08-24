DROP TABLE IF EXISTS table1;

create table table1 (
    col1 Int32,
    col2 Int32
)
ENGINE = MergeTree
partition by tuple()
order by col1;

INSERT INTO table1 VALUES (1, 2), (1, 4);

with NULL as pid
select a.col1, sum(a.col2) as summ
from table1 a
prewhere (pid is null or a.col2 = pid)
group by a.col1;

with 123 as pid
select a.col1, sum(a.col2) as summ
from table1 a
prewhere (pid is null or a.col2 = pid)
group by a.col1;

DROP TABLE table1;
