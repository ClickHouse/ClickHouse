-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t0;

CREATE TABLE t0
(
    c1 Int64,
    c2 Int64,
    c3 Int64,
    PROJECTION p1
    (
        SELECT
            c1,
            c2,
            sum(c3)
        GROUP BY
            c2,
            c1
    )
)
ENGINE = MergeTree ORDER BY (c1, c2) settings min_bytes_for_wide_part = 10485760, min_rows_for_wide_part = 0;

SET optimize_trivial_insert_select = 1;
INSERT INTO t0 SELECT
    number,
    -number,
    number
FROM numbers_mt(1e5);

select * from (EXPLAIN indexes = 1 SELECT c1, sum(c3) FROM t0 GROUP BY c1) where explain like '%ReadFromMergeTree%';
select * from (EXPLAIN indexes = 1 SELECT c1, sum(c3) FROM t0 WHERE c1 = 100 GROUP BY c1) where explain like '%Granules%';

DROP TABLE t0;
