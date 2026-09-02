DROP TABLE IF EXISTS or_expr_bug;
CREATE TABLE or_expr_bug (a UInt64, b UInt64) ENGINE = Memory;

INSERT INTO or_expr_bug VALUES(1,21),(1,22),(1,23),(2,21),(2,22),(2,23),(3,21),(3,22),(3,23);

SELECT count(*) FROM or_expr_bug WHERE (a=1 OR a=2 OR a=3) AND (b=21 OR b=22 OR b=23);
DROP TABLE or_expr_bug;
