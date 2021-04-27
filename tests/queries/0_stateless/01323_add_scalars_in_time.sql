SET optimize_on_insert = 0;

DROP TABLE IF EXISTS tags;

CREATE TABLE tags (
    id String,
    seqs Array(UInt8),
    create_time DateTime DEFAULT now()
) engine=ReplacingMergeTree()
ORDER BY (id);

INSERT INTO tags(id, seqs) VALUES ('id1', [1,2,3]), ('id2', [0,2,3]), ('id1', [1,3]);

WITH
    (SELECT [0, 1, 2, 3]) AS arr1
SELECT arraySort(arrayIntersect(argMax(seqs, create_time), arr1)) AS common, id
FROM tags
WHERE id LIKE 'id%'
GROUP BY id;

DROP TABLE tags;


-- https://github.com/ClickHouse/ClickHouse/issues/15294

drop table if exists TestTable;

create table TestTable (column String, start DateTime, end DateTime) engine MergeTree order by start;

insert into TestTable (column, start, end) values('test', toDateTime('2020-07-20 09:00:00'), toDateTime('2020-07-20 20:00:00')),('test1', toDateTime('2020-07-20 09:00:00'), toDateTime('2020-07-20 20:00:00')),('test2', toDateTime('2020-07-20 09:00:00'), toDateTime('2020-07-20 20:00:00'));

SELECT column,
(SELECT d from (select [1, 2, 3, 4] as d)) as d
FROM TestTable
where column == 'test'
GROUP BY column;

drop table TestTable;

-- https://github.com/ClickHouse/ClickHouse/issues/11407

drop table if exists aaa;
drop table if exists bbb;

CREATE TABLE aaa (
    id UInt16,
    data String
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY id;

INSERT INTO aaa VALUES (1, 'sef'),(2, 'fre'),(3, 'jhg');

CREATE TABLE bbb (
    id UInt16,
    data String
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY id;

INSERT INTO bbb VALUES (2, 'fre'), (3, 'jhg');

with (select groupArray(id) from bbb) as ids
select *
  from aaa
 where has(ids, id)
order by id;


drop table aaa;
drop table bbb;
