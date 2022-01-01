SELECT 1.5::Decimal32(5) + 1.5;
SELECT 1.5::Decimal32(5) - 1.5;
SELECT 1.5::Decimal32(5) * 1.5;
SELECT 1.5::Decimal32(5) / 1.5;

SELECT 1.5 + 1.5::Decimal32(5);
SELECT 1.5 - 1.5::Decimal32(5);
SELECT 1.5 * 1.5::Decimal32(5);
SELECT 1.5 / 1.5::Decimal32(5);

SELECT 1.0::Decimal32(5) / 0.0;

SELECT least(1.5, 1.0::Decimal32(5));
SELECT greatest(1.5, 1.0::Decimal32(5));

DROP TABLE IF EXISTS t;
CREATE TABLE t(d1 Decimal32(5), d2 Decimal64(10), d3 Decimal128(20), d4 Decimal256(40), f1 Float32, f2 Float64) ENGINE=Memory; 

INSERT INTO t values (-4.5, 4.5, -45.5, 45.5, 2.5, -2.5);
INSERT INTO t values (4.5, -4.5, 45.5, -45.5, -2.5, 2.5);
INSERT INTO t values (2.5, -2.5, 25.5, -25.5, -2.5, 2.5);
INSERT INTO t values (-2.5, 2.5, -25.5, 25.5, 2.5, -2.5);

SELECT '';
SELECT 'plus';
SELECT d1, f1, d1 + f1 as plus FROM t ORDER BY plus;
SELECT d2, f1, d2 + f1 as plus FROM t ORDER BY plus;
SELECT d3, f1, d3 + f1 as plus FROM t ORDER BY plus;
SELECT d4, f1, d4 + f1 as plus FROM t ORDER BY plus;

SELECT d1, f2, d1 + f2 as plus FROM t ORDER BY plus;
SELECT d2, f2, d2 + f2 as plus FROM t ORDER BY plus;
SELECT d3, f2, d3 + f2 as plus FROM t ORDER BY plus;
SELECT d4, f2, d4 + f2 as plus FROM t ORDER BY plus;

SELECT '';
SELECT 'minus';
SELECT d1, f1, d1 - f1 as minus FROM t ORDER BY minus;
SELECT d2, f1, d2 - f1 as minus FROM t ORDER BY minus;
SELECT d3, f1, d3 - f1 as minus FROM t ORDER BY minus;
SELECT d4, f1, d4 - f1 as minus FROM t ORDER BY minus;

SELECT d1, f2, d1 - f2 as minus FROM t ORDER BY minus;
SELECT d2, f2, d2 - f2 as minus FROM t ORDER BY minus;
SELECT d3, f2, d3 - f2 as minus FROM t ORDER BY minus;
SELECT d4, f2, d4 - f2 as minus FROM t ORDER BY minus;

SELECT '';
SELECT 'multiply';
SELECT d1, f1, d1 * f1 as multiply FROM t ORDER BY multiply;
SELECT d2, f1, d2 * f1 as multiply FROM t ORDER BY multiply;
SELECT d3, f1, d3 * f1 as multiply FROM t ORDER BY multiply;
SELECT d4, f1, d4 * f1 as multiply FROM t ORDER BY multiply;

SELECT d1, f2, d1 * f2 as multiply FROM t ORDER BY multiply;
SELECT d2, f2, d2 * f2 as multiply FROM t ORDER BY multiply;
SELECT d3, f2, d3 * f2 as multiply FROM t ORDER BY multiply;
SELECT d4, f2, d4 * f2 as multiply FROM t ORDER BY multiply;

SELECT '';
SELECT 'division';
SELECT d1, f1, d1 / f1 as division FROM t ORDER BY division;
SELECT d2, f1, d2 / f1 as division FROM t ORDER BY division;
SELECT d3, f1, d3 / f1 as division FROM t ORDER BY division;
SELECT d4, f1, d4 / f1 as division FROM t ORDER BY division;

SELECT d1, f2, d1 / f2 as division FROM t ORDER BY division;
SELECT d2, f2, d2 / f2 as division FROM t ORDER BY division;
SELECT d3, f2, d3 / f2 as division FROM t ORDER BY division;
SELECT d4, f2, d4 / f2 as division FROM t ORDER BY division;

SELECT '';
SELECT 'least';
SELECT d1, f1, least(d1, f1) as least FROM t ORDER BY least;
SELECT d2, f1, least(d2, f1) as least FROM t ORDER BY least;
SELECT d3, f1, least(d3, f1) as least FROM t ORDER BY least;
SELECT d4, f1, least(d4, f1) as least FROM t ORDER BY least;

SELECT d1, f2, least(d1, f2) as least FROM t ORDER BY least;
SELECT d2, f2, least(d2, f2) as least FROM t ORDER BY least;
SELECT d3, f2, least(d3, f2) as least FROM t ORDER BY least;
SELECT d4, f2, least(d4, f2) as least FROM t ORDER BY least;

SELECT '';
SELECT 'greatest';
SELECT d1, f1, greatest(d1, f1) as greatest FROM t ORDER BY greatest;
SELECT d2, f1, greatest(d2, f1) as greatest FROM t ORDER BY greatest;
SELECT d3, f1, greatest(d3, f1) as greatest FROM t ORDER BY greatest;
SELECT d4, f1, greatest(d4, f1) as greatest FROM t ORDER BY greatest;

SELECT d1, f2, greatest(d1, f2) as greatest FROM t ORDER BY greatest;
SELECT d2, f2, greatest(d2, f2) as greatest FROM t ORDER BY greatest;
SELECT d3, f2, greatest(d3, f2) as greatest FROM t ORDER BY greatest;
SELECT d4, f2, greatest(d4, f2) as greatest FROM t ORDER BY greatest;

DROP TABLE t;
