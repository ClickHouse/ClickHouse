-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/96619
-- When JIT-compiling expressions that convert DateTime to DateTime64,
-- the scale multiplier was not applied, causing the value to be
-- reinterpreted instead of converted (e.g., seconds treated as milliseconds).

SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '--- DateTime to DateTime64 via multiIf ---';

SELECT multiIf(
    number = 0, toNullable(toDateTime('2001-02-03 04:05:06')),
    number = 1, toNullable(toDateTime('2001-02-03 04:05:06')),
    '1970-01-01 00:00:00'::Nullable(DateTime64(3))
) FROM numbers(2);

SELECT '--- DateTime to DateTime64 via if ---';

SELECT if(
    number % 2 = 0,
    toNullable(toDateTime('2001-02-03 04:05:06')),
    '1970-01-01 00:00:00'::Nullable(DateTime64(3))
) FROM numbers(2);

SELECT '--- CASE expression ---';

DROP TABLE IF EXISTS t;
CREATE TABLE t (date1 Nullable(DateTime), date2 Nullable(DateTime)) ENGINE = Memory;
INSERT INTO t(date1, date2) VALUES ('2001-02-03 04:05:06', NULL);

SELECT
    CASE
        WHEN date2 IS NOT NULL THEN date2
        WHEN date1 IS NOT NULL THEN date1
        ELSE '1970-01-01 00:00:00'::Nullable(DateTime64(3))
    END
FROM t;

DROP TABLE t;
