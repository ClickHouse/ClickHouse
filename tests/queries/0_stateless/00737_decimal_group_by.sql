select toDecimal32(1.1, 2) as x group by x;
select toDecimal64(2.1, 4) as x group by x;
select toDecimal128(3.1, 12) as x group by x;

select materialize(toDecimal32(1.2, 2)) as x group by x;
select materialize(toDecimal64(2.2, 4)) as x group by x;
select materialize(toDecimal128(3.2, 12)) as x group by x;

select x from (select toDecimal32(1.3, 2) x) group by x;
select x from (select toDecimal64(2.3, 4) x) group by x;
select x from (select toDecimal128(3.3, 12) x) group by x;

DROP TABLE IF EXISTS decimal;
CREATE TABLE IF NOT EXISTS decimal
(
    A UInt64,
    B Decimal128(18),
    C Decimal128(18)
) Engine = Memory;

INSERT INTO decimal VALUES (1,1,1), (1,1,2), (1,1,3), (1,1,4);

SELECT A, toString(B) AS B_str, toString(SUM(C)) AS c_str FROM decimal GROUP BY A, B_str;
SELECT A, B_str, toString(cc) FROM (SELECT A, toString(B) AS B_str, SUM(C) AS cc FROM decimal GROUP BY A, B_str);

DROP TABLE decimal;
