-- Tags: no-parallel

SELECT * FROM system.numbers WHERE number > toUInt64(10)(number) LIMIT 10; -- { serverError 309 }

CREATE FUNCTION IF NOT EXISTS sum_udf as (x, y) -> (x + y);

SELECT sum_udf(1)(1, 2); -- { serverError 309 }

DROP FUNCTION IF EXISTS sum_udf;
