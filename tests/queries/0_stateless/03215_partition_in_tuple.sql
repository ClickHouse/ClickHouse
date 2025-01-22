CREATE OR REPLACE TABLE t(a DateTime64(3), b FixedString(32))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT '2023-01-01 00:00:00.000', 'fd4c0374925304dd2760dacb88d7b117';

SELECT * FROM t WHERE tuple(a, b) IN (SELECT tuple(a, b) FROM t);
SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM t);


CREATE OR REPLACE TABLE t(a DateTime, b FixedString(32))
ENGINE = MergeTree
PARTITION BY toStartOfDay(a)
ORDER BY (a, b)
AS SELECT '2023-01-01 00:00:00', 'fd4c0374925304dd2760dacb88d7b117';

SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM t);
SELECT * FROM t WHERE tuple(a, b) IN (SELECT tuple(a, b) FROM t);

