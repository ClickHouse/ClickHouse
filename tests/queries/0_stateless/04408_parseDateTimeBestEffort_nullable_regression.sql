-- Regression: parseDateTimeBestEffort(toString(nullable_col), timezone) must not throw on NULL rows.
-- the parser must skip empty strings at null positions
CREATE TABLE t_nullable_dt (c1 Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_dt VALUES ('2024-03-15 10:20:30.123'), (NULL), ('2023-01-02 03:04:05.000');

SELECT parseDateTimeBestEffort(toString(c1), 'UTC') IS NULL AS is_null FROM t_nullable_dt ORDER BY c1 NULLS LAST;

SELECT parseDateTimeBestEffort(toString(c1)) IS NULL AS is_null FROM t_nullable_dt ORDER BY c1 NULLS LAST;

SELECT parseDateTimeBestEffortOrNull(toString(c1), 'UTC') IS NULL AS is_null FROM t_nullable_dt ORDER BY c1 NULLS LAST;

DROP TABLE t_nullable_dt;
