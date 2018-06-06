drop table IF EXISTS test.joinbug;

CREATE TABLE test.joinbug (
  event_date Date MATERIALIZED toDate(created, 'Europe/Moscow'),
  id UInt64,
  id2 UInt64,
  val UInt64,
  val2 Int32,
  created UInt64
) ENGINE = MergeTree(event_date, (id, id2), 8192);

insert into test.joinbug (id, id2, val, val2, created) values (1,11,91,81,123456), (2,22,92,82,123457);

drop table IF EXISTS test.joinbug_join;

CREATE TABLE test.joinbug_join (
  id UInt64,
  id2 UInt64,
  val UInt64,
  val2 Int32,
  created UInt64
) ENGINE = Join(ANY, INNER, id2);

insert into test.joinbug_join (id, id2, val, val2, created)
select id, id2, val, val2, created
from test.joinbug;

/* expected */
select *
from test.joinbug;

/* wtf */
select id, id2, val, val2, created
from (
   SELECT toUInt64(arrayJoin(range(50))) AS id2
)
ANY INNER JOIN test.joinbug_join using id2;

DROP TABLE test.joinbug;
DROP TABLE test.joinbug_join;
