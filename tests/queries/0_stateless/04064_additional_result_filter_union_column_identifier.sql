-- Tags: no-random-settings
-- `UNION ALL` with duplicate values followed by `ORDER BY a`: ties within each
-- value group have undefined order, sensitive to pipeline shape changes from
-- `max_streams_for_union_step` etc.

-- Regression test for "Column identifier is already registered" exception
-- when additional_result_filter is used with UNION/EXCEPT queries.
-- https://github.com/ClickHouse/ClickHouse/issues/99931

DROP TABLE IF EXISTS t_04064;
CREATE TABLE t_04064 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04064 VALUES (1), (2), (3);

SELECT * FROM (SELECT x AS a FROM t_04064)
EXCEPT DISTINCT
SELECT * FROM (SELECT x AS a FROM t_04064)
SETTINGS additional_result_filter = 'a != 3';

SELECT * FROM
(
    SELECT * FROM (SELECT x AS a FROM t_04064)
    UNION ALL
    SELECT * FROM (SELECT x AS a FROM t_04064)
)
ORDER BY a
SETTINGS additional_result_filter = 'a != 3';

DROP TABLE t_04064;
