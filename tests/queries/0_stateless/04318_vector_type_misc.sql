-- Tests general behavior of the Vector(T, N) data type: ordering, grouping, formats, parameters.

SET allow_experimental_vector_type = 1;

DROP TABLE IF EXISTS t_vector_misc;
CREATE TABLE t_vector_misc (id UInt32, v Vector(Float32, 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector_misc VALUES (1, [2, 1]), (2, [1, 2]), (3, [1, 10]), (4, [1, 2]), (5, [-1, 100]);

SELECT '-- ORDER BY follows element-wise (lexicographic) numeric order';
SELECT v FROM t_vector_misc ORDER BY v, id;

SELECT '-- GROUP BY and DISTINCT';
SELECT v, count() FROM t_vector_misc GROUP BY v ORDER BY v;
SELECT DISTINCT v FROM t_vector_misc ORDER BY v;
SELECT uniqExact(v) FROM t_vector_misc;

SELECT '-- comparison is element-wise';
SELECT CAST([1, 2] AS Vector(Float32, 2)) = CAST([1, 2] AS Vector(Float32, 2));
SELECT CAST([1, 2] AS Vector(Float32, 2)) < CAST([2, 1] AS Vector(Float32, 2));
SELECT min(v), max(v) FROM t_vector_misc;

SELECT '-- formats round-trip';
SELECT v FROM t_vector_misc ORDER BY id LIMIT 2 FORMAT CSV;
SELECT v FROM t_vector_misc ORDER BY id LIMIT 2 FORMAT JSONEachRow;
SELECT v FROM t_vector_misc ORDER BY id LIMIT 2 FORMAT TSV;

DROP TABLE IF EXISTS t_vector_csv;
CREATE TABLE t_vector_csv (v Vector(Float32, 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_vector_csv FORMAT CSV "[1.5, 2.5]"

INSERT INTO t_vector_csv FORMAT JSONEachRow {"v": [3.5, 4.5]}

SELECT v FROM t_vector_csv ORDER BY v;
DROP TABLE t_vector_csv;

SELECT '-- query parameters keep their literal form';
SET param_qv = [1, 2];
SELECT {qv:Vector(Float32, 2)} AS v, toTypeName(v);

SELECT '-- default value is a zero vector';
DROP TABLE IF EXISTS t_vector_def;
CREATE TABLE t_vector_def (id UInt32, v Vector(Float32, 3)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_vector_def (id) VALUES (1);
SELECT id, v FROM t_vector_def;
DROP TABLE t_vector_def;

DROP TABLE t_vector_misc;
