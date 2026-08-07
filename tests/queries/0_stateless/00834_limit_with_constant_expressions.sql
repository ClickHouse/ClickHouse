SELECT number FROM numbers(10) LIMIT 0 + 1;
SELECT number FROM numbers(10) LIMIT 1 - 1;
SELECT number FROM numbers(10) LIMIT 2 - 1;
SELECT number FROM numbers(10) LIMIT 0 - 1;
SELECT number FROM numbers(10) LIMIT 1.0;
SELECT number FROM numbers(10) LIMIT 1.5; -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT number FROM numbers(10) LIMIT '1'; -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT number FROM numbers(10) LIMIT now(); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT number FROM numbers(10) LIMIT today(); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT number FROM numbers(10) LIMIT toUInt8('1');
SELECT number FROM numbers(10) LIMIT toFloat32('1');
SELECT number FROM numbers(10) LIMIT rand(); -- { serverError BAD_ARGUMENTS, INVALID_LIMIT_EXPRESSION }

SELECT count() <= 1 FROM (SELECT number FROM numbers(10) LIMIT randConstant() % 2);

SELECT number FROM numbers(10) LIMIT 0 + 1 BY number;
SELECT number FROM numbers(10) LIMIT 0 BY number;

SELECT TOP 5 * FROM numbers(10);

SELECT * FROM numbers(10) LIMIT 0.33 / 0.165 - 0.33 + 0.67; -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM numbers(10) LIMIT LENGTH('NNN') + COS(0), toDate('0000-00-02'); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM numbers(10) LIMIT LENGTH('NNN') + COS(0), toDate('0000-00-02'); -- { serverError INVALID_LIMIT_EXPRESSION }
SELECT * FROM numbers(10) LIMIT a + 5 - a; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM numbers(10) LIMIT a + b; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM numbers(10) LIMIT 'Hello'; -- { serverError INVALID_LIMIT_EXPRESSION }

SELECT number from numbers(10) order by number limit (select sum(number), count() from numbers(3)).1;
