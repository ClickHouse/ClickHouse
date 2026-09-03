DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1
(
    `id1` UInt64,
    `id2` UInt8,
)
ENGINE = MergeTree ORDER BY (id1)
AS SELECT 1, 1;

CREATE TABLE table2 (id1 UInt64, id2 UInt8) ENGINE = Memory as select 1, 1;

select * from table1 where (id1, id2) in (select tuple(id1, id2) from table2);

DROP TABLE table1;
DROP TABLE table2;
