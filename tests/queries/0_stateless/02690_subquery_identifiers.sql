DROP TABLE IF EXISTS t_str;

CREATE TABLE t_str
(
    `creation_time` String
)
ENGINE = MergeTree
PARTITION BY creation_time
ORDER BY creation_time;

insert into t_str values ('2020-02-02');

select 1 as x from t_str where cast('1970-01-01' as date) <= cast((select max('1970-01-01') from numbers(1)) as date);
select * from ( select 1 as x from t_str where cast('1970-01-01' as date) <= cast((select max('1970-01-01') from numbers(1)) as date));
SELECT * FROM (SELECT * FROM t_str WHERE (SELECT any('1970-01-01'))::Date > today());

DROP TABLE t_str;
