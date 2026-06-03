SET any_join_distinct_right_table_keys = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1_00848;
DROP TABLE IF EXISTS t2_00848;
DROP TABLE IF EXISTS t3_00848;
CREATE TABLE t1_00848 ( id String ) ENGINE = Memory;
CREATE TABLE t2_00848 ( id Nullable(String) ) ENGINE = Memory;
CREATE TABLE t3_00848 ( id Nullable(String), not_id Nullable(String) ) ENGINE = Memory;

insert into t1_00848 values ('l');
insert into t3_00848 (id) values ('r');

SELECT 'on';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY t1.id, t3.id;

SET join_use_nulls = 1;

SELECT 'on + join_use_nulls';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 ON t2.id = t3.id ORDER BY t2.id, t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 ON t1.id = t3.id ORDER BY t1.id, t3.id;

SELECT 'using + join_use_nulls';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 ANY FULL JOIN t3_00848 t3 USING(id) ORDER BY id;

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1_00848 t1 FULL JOIN t3_00848 t3 USING(id) ORDER BY id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2_00848 t2 FULL JOIN t3_00848 t3 USING(id) ORDER BY id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 ANY LEFT JOIN t3_00848 t3 USING(id) ORDER BY id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1_00848 t1 LEFT JOIN t3_00848 t3 USING(id) ORDER BY id;

DROP TABLE t1_00848;
DROP TABLE t2_00848;
DROP TABLE t3_00848;
