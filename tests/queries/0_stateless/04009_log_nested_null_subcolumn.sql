-- Reproducer for "Invalid number of columns in chunk pushed to OutputPort"
-- when reading both a Nested element and its `.null` subcolumn from Log engine.
-- The `.null` subcolumn must use the Nested serialization (with shared offsets),
-- not the standalone Array(Nullable(T)) serialization.

SET allow_suspicious_low_cardinality_types = 1;
SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nested_null_sub;
CREATE TABLE t_nested_null_sub (c0 Nested(c1 LowCardinality(IPv4), c2 Nullable(Tuple())), c3 DateTime64(4) NULL) ENGINE = Log;
INSERT INTO t_nested_null_sub (`c0.c1`, `c0.c2`, c3) VALUES (['127.0.0.1', '10.0.0.1'], [NULL, tuple()], '2024-01-01 00:00:00.0000');
SELECT `c0.c1`, `c0.c2`, `c0.c2.null` FROM t_nested_null_sub;
SELECT `c0.c2.null` FROM t_nested_null_sub;
SELECT `c0.c2`, `c0.c2.null` FROM t_nested_null_sub;

DROP TABLE IF EXISTS t_nested_null_sub_tiny;
CREATE TABLE t_nested_null_sub_tiny (c0 Nested(c1 LowCardinality(IPv4), c2 Nullable(Tuple())), c3 DateTime64(4) NULL) ENGINE = TinyLog;
INSERT INTO t_nested_null_sub_tiny (`c0.c1`, `c0.c2`, c3) VALUES (['127.0.0.1', '10.0.0.1'], [NULL, tuple()], '2024-01-01 00:00:00.0000');
SELECT `c0.c1`, `c0.c2`, `c0.c2.null` FROM t_nested_null_sub_tiny;
SELECT `c0.c2.null` FROM t_nested_null_sub_tiny;
SELECT `c0.c2`, `c0.c2.null` FROM t_nested_null_sub_tiny;

DROP TABLE t_nested_null_sub;
DROP TABLE t_nested_null_sub_tiny;
