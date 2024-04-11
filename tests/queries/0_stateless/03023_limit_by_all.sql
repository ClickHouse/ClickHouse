-- Tests that sort expression ORDER BY ALL

DROP TABLE IF EXISTS limit_by_all;

CREATE TABLE limit_by_all
(
    a String,
    b Int32
)
ENGINE = Memory;

INSERT INTO limit_by_all VALUES ('B', 1), ('B', 2) ('A', 1), ('A', 2);

SELECT '-- no modifiers';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;
SELECT a, b FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY ALL;
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 2 BY all;
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1,1 BY all;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;
SELECT a, b FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY ALL;
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 2 BY all;
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1,1 BY all;

SELECT '-- SELECT *';

SET allow_experimental_analyzer = 0;
SELECT * FROM limit_by_all ORDER BY a,b ASC LIMIT 2 BY all;

SET allow_experimental_analyzer = 1;
SELECT * FROM limit_by_all ORDER BY a,b ASC LIMIT 2 BY all;

DROP TABLE limit_by_all;

SELECT '-- using "limit by all" with column named by all is ambiguous';

CREATE TABLE limit_by_all
(
    a String,
    b Int32,
    all UInt64
)
ENGINE = Memory;

INSERT INTO limit_by_all VALUES ('B', 1 , 10), ('B', 2, 20), ('A', 1, 10), ('A', 2, 20);

SELECT '  -- columns named all';

SET allow_experimental_analyzer = 0;
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;
SELECT * FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT * FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;


SET allow_experimental_analyzer = 1;
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;
-- SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a FROM limit_by_all ORDER BY a ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;
-- SELECT * FROM limit_by_all ORDER BY a ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT * FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;

-- (*) These queries show the expected behavior for analyzer. Unfortunately, it is not implemented that way yet,
-- which is not wrong but a bit unintuitive (some may say a landmine). Keeping the queries for now for reference.

SELECT '  -- column aliases';

SET allow_experimental_analyzer = 0;
SELECT a, b AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;

SET allow_experimental_analyzer = 1;
SELECT a, b AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;

SELECT '  -- expressions';

SET allow_experimental_analyzer = 0;
SELECT format('{} {}', a, b) AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT format('{} {}', a, b) AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;

SET allow_experimental_analyzer = 1;
SELECT format('{} {}', a, b) AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT format('{} {}', a, b) AS all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all SETTINGS enable_limit_by_all = false;

SELECT '  -- LIMIT BY ALL loses its special meaning when used in conjunction with other columns';

SET allow_experimental_analyzer = 0;
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all,a;

SET allow_experimental_analyzer = 1;
SELECT a, b, all FROM limit_by_all ORDER BY a,b ASC LIMIT 1 BY all,a;

DROP TABLE limit_by_all;
