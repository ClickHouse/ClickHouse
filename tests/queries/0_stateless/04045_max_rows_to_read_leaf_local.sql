-- max_rows_to_read_leaf should not apply to local (non-distributed) queries
-- See https://github.com/ClickHouse/ClickHouse/issues/99268

DROP TABLE IF EXISTS t_leaf_limit;
CREATE TABLE t_leaf_limit (number UInt32) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_leaf_limit SELECT number FROM numbers(10000000);

CREATE VIEW v_leaf_limit AS SELECT * FROM t_leaf_limit;

-- Direct table access should work (leaf limit should not apply locally)
SELECT count() FROM t_leaf_limit SETTINGS max_rows_to_read_leaf = 5000000;

-- View with ORDER BY should work
SELECT count() FROM (SELECT * FROM v_leaf_limit ORDER BY number LIMIT 3) SETTINGS max_rows_to_read_leaf = 5000000;

-- View without ORDER BY should also work (this was the failing case)
SELECT count() FROM (SELECT * FROM v_leaf_limit LIMIT 3) SETTINGS max_rows_to_read_leaf = 5000000;

DROP VIEW v_leaf_limit;
DROP TABLE t_leaf_limit;
