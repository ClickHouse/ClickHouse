DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (`a` UInt32, `b` UInt32, `c` UInt32 ) ENGINE = Memory;
INSERT INTO t1 VALUES (1, 1, 1), (1, 1, 2), (2, 2, 2), (1, 2, 2);

SELECT DISTINCT ON (a, b) a, b, c FROM t1;
SELECT DISTINCT ON (a, b) * FROM t1;
SELECT DISTINCT ON (a) * FROM t1;

-- fuzzer will fail, enable when fixed
-- SELECT DISTINCT ON (a, b) a, b, c FROM t1 LIMIT 1 BY a, b; -- { clientError SYNTAX_ERROR }

-- SELECT DISTINCT ON a, b a, b FROM t1; -- { clientError SYNTAX_ERROR }
-- SELECT DISTINCT ON a a, b FROM t1; -- { clientError SYNTAX_ERROR }

-- "Code: 47. DB::Exception: Missing columns: 'DISTINCT'" - error can be better
-- SELECT DISTINCT ON (a, b) DISTINCT a, b FROM t1; -- { serverError UNKNOWN_IDENTIFIER }
-- SELECT DISTINCT DISTINCT ON (a, b) a, b FROM t1; -- { clientError SYNTAX_ERROR }

-- SELECT ALL DISTINCT ON (a, b) a, b FROM t1; -- { clientError SYNTAX_ERROR }
-- SELECT DISTINCT ON (a, b) ALL a, b FROM t1; -- { clientError SYNTAX_ERROR }

DROP TABLE IF EXISTS t1;
