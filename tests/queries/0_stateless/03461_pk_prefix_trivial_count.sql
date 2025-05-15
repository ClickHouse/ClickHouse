-- { echo ON }

DROP TABLE IF EXISTS t;

CREATE TABLE t(k String) ORDER BY k as select 'dst_'||number from numbers(1e6);

SELECT count(*) FROM t WHERE k LIKE 'dst_kkkk_1111%';

DROP TABLE t;
