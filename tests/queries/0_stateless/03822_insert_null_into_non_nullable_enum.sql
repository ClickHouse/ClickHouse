-- https://github.com/ClickHouse/ClickHouse/issues/69683
-- Inserting NULL into a non-Nullable Enum column should use the default value,
-- not produce an invalid value that causes errors later.

DROP TABLE IF EXISTS t_enum_null;
CREATE TABLE t_enum_null (c0 Enum('a' = 1)) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY (c0);
INSERT INTO t_enum_null (c0) VALUES (NULL);
SELECT * FROM t_enum_null;
DROP TABLE t_enum_null;
