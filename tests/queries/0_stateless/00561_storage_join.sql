drop table IF EXISTS joinbug;

CREATE TABLE joinbug (
  event_date Date MATERIALIZED toDate(created, 'Europe/Moscow'),
  id UInt64,
  id2 UInt64,
  val UInt64,
  val2 Int32,
  created UInt64
) ENGINE = MergeTree(event_date, (id, id2), 8192);

insert into joinbug (id, id2, val, val2, created) values (1,11,91,81,123456), (2,22,92,82,123457);

drop table IF EXISTS joinbug_join;

CREATE TABLE joinbug_join (
  id UInt64,
  id2 UInt64,
  val UInt64,
  val2 Int32,
  created UInt64
) ENGINE = Join(SEMI, LEFT, id2);

insert into joinbug_join (id, id2, val, val2, created)
select id, id2, val, val2, created
from joinbug;

select * from joinbug;

select id, id2, val, val2, created
from ( SELECT toUInt64(arrayJoin(range(50))) AS id2 ) js1
SEMI LEFT JOIN joinbug_join using id2;

-- type conversion
SELECT * FROM ( SELECT toUInt32(11) AS id2 ) AS js1 SEMI LEFT JOIN joinbug_join USING (id2);

-- can't convert right side in case on storage join
SELECT * FROM ( SELECT toInt64(11) AS id2 ) AS js1 SEMI LEFT JOIN joinbug_join USING (id2); -- { serverError 53 }

DROP TABLE joinbug;
DROP TABLE joinbug_join;
