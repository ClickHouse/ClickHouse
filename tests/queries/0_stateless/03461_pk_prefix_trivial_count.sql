-- { echo ON }

DROP TABLE IF EXISTS t;

CREATE TABLE t(k String) ORDER BY k as select 'dst_'||number from numbers(10);

SELECT count(*) FROM t WHERE k LIKE 'dst_kkkk_1111%';

SELECT count(*) FROM t WHERE k LIKE 'dst%kkkk';

DROP TABLE t;
