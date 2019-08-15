DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1
(
dt Date,
id Int32,
arr Array(LowCardinality(String))
) ENGINE = MergeTree PARTITION BY toMonday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

CREATE TABLE table2
(
dt Date,
id Int32,
arr Array(LowCardinality(String))
) ENGINE = MergeTree PARTITION BY toMonday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

insert into table1 (dt, id, arr) values ('2019-01-14', 1, ['aaa']);
insert into table2 (dt, id, arr) values ('2019-01-14', 1, ['aaa','bbb','ccc']);

select dt, id, arraySort(groupArrayArray(arr))
from (
    select dt, id, arr from table1
    where dt = '2019-01-14' and id = 1
    UNION ALL
    select dt, id, arr from table2
    where dt = '2019-01-14' and id = 1
)
group by dt, id;

DROP TABLE table1;
DROP TABLE table2;
