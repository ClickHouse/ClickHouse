SELECT groupBitmapAnd(z) y FROM ( SELECT groupBitmapState(u) AS z FROM ( SELECT 123 AS u ) AS a1 );
SELECT groupBitmapAnd(y) FROM (SELECT groupBitmapAndState(z) y FROM ( SELECT groupBitmapState(u) AS z FROM ( SELECT 123 AS u ) AS a1 ) AS a2);

SELECT groupBitmapAnd(z) FROM ( SELECT minState(u) AS z FROM ( SELECT 123 AS u ) AS a1 ) AS a2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupBitmapOr(z) FROM ( SELECT maxState(u) AS z FROM ( SELECT '123' AS u ) AS a1 ) AS a2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT groupBitmapXor(z) FROM ( SELECT countState() AS z FROM ( SELECT '123' AS u ) AS a1 ) AS a2; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
