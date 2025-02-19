DROP TABLE IF EXISTS dtest;


CREATE TABLE dtest ( `a` Decimal(18, 0), `b` Decimal(18, 1), `c` Decimal(36, 0) ) ENGINE = Memory;
INSERT INTO dtest VALUES ('33', '44.4', '35');

SELECT count() == 0 FROM (SELECT '33.3' :: Decimal(9, 1) AS a WHERE a IN ('33.33' :: Decimal(9, 2)));

SELECT count() == 0 FROM dtest WHERE a IN toDecimal32('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal64('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal128('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal256('33.3000', 4);

SELECT count() == 0 FROM dtest WHERE b IN toDecimal32('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal64('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal128('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal256('44.4000', 0);

SELECT count() == 1 FROM dtest WHERE b IN toDecimal32('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal64('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal128('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal256('44.4000', 4);

SET enable_analyzer = 1;

SELECT count() == 0 FROM (SELECT '33.3' :: Decimal(9, 1) AS a WHERE a IN ('33.33' :: Decimal(9, 2)));

SELECT count() == 0 FROM dtest WHERE a IN toDecimal32('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal64('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal128('33.3000', 4);
SELECT count() == 0 FROM dtest WHERE a IN toDecimal256('33.3000', 4);

SELECT count() == 0 FROM dtest WHERE b IN toDecimal32('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal64('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal128('44.4000', 0);
SELECT count() == 0 FROM dtest WHERE b IN toDecimal256('44.4000', 0);

SELECT count() == 1 FROM dtest WHERE b IN toDecimal32('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal64('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal128('44.4000', 4);
SELECT count() == 1 FROM dtest WHERE b IN toDecimal256('44.4000', 4);

DROP TABLE IF EXISTS dtest;
