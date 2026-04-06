-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/76182
-- Distributed query with IN in the SELECT section should not throw
-- ILLEGAL_TYPE_OF_ARGUMENT when using the analyzer.

DROP TABLE IF EXISTS t_dist_in_select;

CREATE TABLE t_dist_in_select
(
    uid UInt32
) ENGINE = MergeTree() ORDER BY uid;

INSERT INTO t_dist_in_select VALUES (1), (2), (3);

SELECT uid IN (1, 3)
FROM remote('127.0.0.1', currentDatabase(), t_dist_in_select)
ORDER BY uid;

DROP TABLE t_dist_in_select;
