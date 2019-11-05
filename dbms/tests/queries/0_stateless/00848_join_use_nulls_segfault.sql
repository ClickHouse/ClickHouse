SET any_join_distinct_right_table_keys = 1;

DROP TABLE IF EXISTS t1_00848;
DROP TABLE IF EXISTS t2_00848;
DROP TABLE IF EXISTS t3_00848;
CREATE TABLE t1_00848 ( id String ) ENGINE = Memory;
CREATE TABLE t2_00848 ( id Nullable(String) ) ENGINE = Memory;
CREATE TABLE t3_00848 ( id Nullable(String), not_id Nullable(String) ) ENGINE = Memory;

insert into t1_00848 values ('l');
insert into t3_00848 (id) values ('r');

SELECT 'on';

SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 ANY LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 ANY FULL JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 ANY FULL JOIN t3_00848 ON t2_00848.id = t3_00848.id;

SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 FULL JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 FULL JOIN t3_00848 ON t2_00848.id = t3_00848.id;

SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 ANY LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;

SELECT 'using';

SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 ANY LEFT JOIN t3_00848 USING(id);
SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 ANY FULL JOIN t3_00848 USING(id);
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 ANY FULL JOIN t3_00848 USING(id);

SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 LEFT JOIN t3_00848 USING(id);
SELECT *, toTypeName(t1_00848.id), toTypeName(t3_00848.id) FROM t1_00848 FULL JOIN t3_00848 USING(id);
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 FULL JOIN t3_00848 USING(id);

SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 ANY LEFT JOIN t3_00848 USING(id);
SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 LEFT JOIN t3_00848 USING(id);

SET join_use_nulls = 1;
-- TODO: toTypeName(t1_00848.id) String -> Nullable(String)

SELECT 'on + join_use_nulls';

SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 ANY LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 ANY FULL JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 ANY FULL JOIN t3_00848 ON t2_00848.id = t3_00848.id;

SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 FULL JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 FULL JOIN t3_00848 ON t2_00848.id = t3_00848.id;

SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 ANY LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;
SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 LEFT JOIN t3_00848 ON t1_00848.id = t3_00848.id;

SELECT 'using + join_use_nulls';

SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 ANY LEFT JOIN t3_00848 USING(id);
SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 ANY FULL JOIN t3_00848 USING(id);
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 ANY FULL JOIN t3_00848 USING(id);

SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 LEFT JOIN t3_00848 USING(id);
SELECT *, 'TODO', toTypeName(t3_00848.id) FROM t1_00848 FULL JOIN t3_00848 USING(id);
SELECT *, toTypeName(t2_00848.id), toTypeName(t3_00848.id) FROM t2_00848 FULL JOIN t3_00848 USING(id);

SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 ANY LEFT JOIN t3_00848 USING(id);
SELECT t3_00848.id = 'l', t3_00848.not_id = 'l' FROM t1_00848 LEFT JOIN t3_00848 USING(id);

DROP TABLE t1_00848;
DROP TABLE t2_00848;
DROP TABLE t3_00848;
