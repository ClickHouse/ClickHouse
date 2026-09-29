SET convert_query_to_cnf = 0;

DROP TABLE IF EXISTS t0;

CREATE TABLE t0
(
    `c0` Int32,
    `c1` Int32 CODEC(NONE)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192;

INSERT INTO t0 VALUES (0, 0);

SELECT t0.c1 FROM t0 WHERE NOT (t0.c1 OR (t0.c0 AND -1524532316));
-- A constant floating point argument of a logical function is converted to a boolean by truthiness,
-- the same way a non-constant one is, so this does not throw, see issue #71904.
SELECT t0.c1 FROM t0 WHERE NOT (t0.c1 OR (t0.c0 AND -1.0));
SELECT t0.c1 FROM t0 WHERE NOT (t0.c1 OR (t0.c0 AND inf));
SELECT t0.c1 FROM t0 WHERE NOT (t0.c1 OR (t0.c0 AND nan));

DROP TABLE t0;
