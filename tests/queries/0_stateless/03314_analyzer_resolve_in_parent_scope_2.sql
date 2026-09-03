create table test
(
    raw_id String,
    columns_n Nested
        (
        col_1 Nullable(String),
        col_2 Nullable(String)
        )
)
Engine = MergeTree
order by (raw_id);

insert into test
VALUES('1', ['type_1','type_2','type_1'],['0','0','1']),
      ('2', ['type_3','type_2','type_1'],['0','1','1']),
      ('3', ['type_1','type_2','type_3'],['1','0','1'])
;

with t like '%_1%' as issue
select
    raw_id,
    arrayFilter((t, t2) -> (not issue),
        columns_n.col_1,
        columns_n.col_2
    )
from test
order by raw_id;
