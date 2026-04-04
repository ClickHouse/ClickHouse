-- https://github.com/ClickHouse/ClickHouse/issues/83434

DROP TABLE IF EXISTS t_array_join_alias;

CREATE TABLE t_array_join_alias (
    a Map(UInt64, UInt64),
    `a.k` Array(UInt64) ALIAS mapKeys(a)
) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_array_join_alias VALUES (map(1, 2)), (map(3, 4));

-- Previously this query failed with "Unknown expression identifier `aa.k`"
-- because the analyzer did not fall back to resolving ALIAS columns from the
-- table schema when the array join alias matched a column name prefix.
SELECT aa.k FROM t_array_join_alias ARRAY JOIN a AS aa ORDER BY aa.k;

DROP TABLE t_array_join_alias;
