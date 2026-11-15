DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_v;

CREATE TABLE tab (id Int32, val Nullable(Float64), dt Nullable(DateTime64(6)), type Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

insert into tab values (1,10,'2023-01-14 00:00:00',1),(2,20,'2023-01-14 00:00:00',1),(3,20,'2023-01-14 00:00:00',2),(4,40,'2023-01-14 00:00:00',3),(5,50,'2023-01-14 00:00:00',3);

CREATE VIEW tab_v AS SELECT
    t1.type AS type,
    sum(t1.val) AS sval,
    toStartOfDay(t1.dt) AS sday,
    anyLast(sval) OVER w AS lval
FROM tab AS t1
GROUP BY
    type,
    sday
WINDOW w AS (PARTITION BY type);

select distinct type from tab_v order by type;
select '--------';
select distinct type, sday from tab_v order by type, sday;
