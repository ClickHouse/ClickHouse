DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 ( id String ) ENGINE = Memory;
CREATE TABLE t2 ( id Nullable(String) ) ENGINE = Memory;
CREATE TABLE t3 ( id Nullable(String), not_id Nullable(String) ) ENGINE = Memory;

insert into t1 values ('l');
insert into t3 (id) values ('r');

SELECT 'on';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 ANY LEFT JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 ANY FULL JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 ANY FULL JOIN t3 ON t2.id = t3.id;

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 LEFT JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 FULL JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 FULL JOIN t3 ON t2.id = t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 ANY LEFT JOIN t3 ON t1.id = t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 LEFT JOIN t3 ON t1.id = t3.id;

SELECT 'using';

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 ANY LEFT JOIN t3 USING(id);
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 ANY FULL JOIN t3 USING(id);
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 ANY FULL JOIN t3 USING(id);

SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 LEFT JOIN t3 USING(id);
SELECT *, toTypeName(t1.id), toTypeName(t3.id) FROM t1 FULL JOIN t3 USING(id);
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 FULL JOIN t3 USING(id);

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 ANY LEFT JOIN t3 USING(id);
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 LEFT JOIN t3 USING(id);

SET join_use_nulls = 1;
-- TODO: toTypeName(t1.id) String -> Nullable(String)

SELECT 'on + join_use_nulls';

SELECT *, 'TODO', toTypeName(t3.id) FROM t1 ANY LEFT JOIN t3 ON t1.id = t3.id;
SELECT *, 'TODO', toTypeName(t3.id) FROM t1 ANY FULL JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 ANY FULL JOIN t3 ON t2.id = t3.id;

SELECT *, 'TODO', toTypeName(t3.id) FROM t1 LEFT JOIN t3 ON t1.id = t3.id;
SELECT *, 'TODO', toTypeName(t3.id) FROM t1 FULL JOIN t3 ON t1.id = t3.id;
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 FULL JOIN t3 ON t2.id = t3.id;

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 ANY LEFT JOIN t3 ON t1.id = t3.id;
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 LEFT JOIN t3 ON t1.id = t3.id;

SELECT 'using + join_use_nulls';

SELECT *, 'TODO', toTypeName(t3.id) FROM t1 ANY LEFT JOIN t3 USING(id);
SELECT *, 'TODO', toTypeName(t3.id) FROM t1 ANY FULL JOIN t3 USING(id);
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 ANY FULL JOIN t3 USING(id);

SELECT *, 'TODO', toTypeName(t3.id) FROM t1 LEFT JOIN t3 USING(id);
SELECT *, 'TODO', toTypeName(t3.id) FROM t1 FULL JOIN t3 USING(id);
SELECT *, toTypeName(t2.id), toTypeName(t3.id) FROM t2 FULL JOIN t3 USING(id);

SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 ANY LEFT JOIN t3 USING(id);
SELECT t3.id = 'l', t3.not_id = 'l' FROM t1 LEFT JOIN t3 USING(id);

DROP TABLE t1;
DROP TABLE t2;
