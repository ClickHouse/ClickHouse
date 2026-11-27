-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/81636
-- UNION ALL with remote() and an outer WHERE clause should not fail with
-- UNKNOWN_DATABASE when predicate pushdown tries to resolve the remote database locally.

DROP TABLE IF EXISTS t_union_remote;

CREATE TABLE t_union_remote
(
    key UInt64
) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t_union_remote VALUES (1), (2), (3);

SELECT count()
FROM
(
    SELECT key
    FROM t_union_remote
    UNION ALL
    SELECT key
    FROM remote('127.0.0.1', currentDatabase(), t_union_remote)
)
WHERE key = 1;

DROP TABLE t_union_remote;
