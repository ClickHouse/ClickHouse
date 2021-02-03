SELECT number FROM numbers(10) LIMIT 0 + 1;
SELECT number FROM numbers(10) LIMIT 1 - 1;
SELECT number FROM numbers(10) LIMIT 2 - 1;
SELECT number FROM numbers(10) LIMIT 0 - 1; -- { serverError 440 }
SELECT number FROM numbers(10) LIMIT 1.0;
SELECT number FROM numbers(10) LIMIT 1.5; -- { serverError 440 }
SELECT number FROM numbers(10) LIMIT '1'; -- { serverError 440 }
SELECT number FROM numbers(10) LIMIT now(); -- { serverError 440 }
SELECT number FROM numbers(10) LIMIT today(); -- { serverError 440 }
SELECT number FROM numbers(10) LIMIT toUInt8('1');
SELECT number FROM numbers(10) LIMIT toFloat32('1');
SELECT number FROM numbers(10) LIMIT rand(); -- { serverError 36 }

SELECT count() <= 1 FROM (SELECT number FROM numbers(10) LIMIT randConstant() % 2);

SELECT number FROM numbers(10) LIMIT 0 + 1 BY number;
SELECT number FROM numbers(10) LIMIT 0 BY number;

SELECT TOP 5 * FROM numbers(10);

SELECT * FROM numbers(10) LIMIT 0.33 / 0.165 - 0.33 + 0.67; -- { serverError 440 }
SELECT * FROM numbers(10) LIMIT LENGTH('NNN') + COS(0), toDate('0000-00-02'); -- { serverError 440 }
SELECT * FROM numbers(10) LIMIT LENGTH('NNN') + COS(0), toDate('0000-00-02'); -- { serverError 440 }
SELECT * FROM numbers(10) LIMIT a + 5 - a; -- { serverError 47 }
SELECT * FROM numbers(10) LIMIT a + b; -- { serverError 47 }
SELECT * FROM numbers(10) LIMIT 'Hello'; -- { serverError 440 }
