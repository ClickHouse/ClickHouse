-- The trivial `GROUP BY ... LIMIT` optimization with aggregate functions also covers
-- reading from an aggregate projection: there the streams merge pre-aggregated states
-- (`Aggregator::mergeOnBlock`) instead of aggregating raw rows, and the shared kept-keys
-- cutoff must restrict them the same way. The values of the kept keys must match the
-- full aggregation exactly.

DROP TABLE IF EXISTS t_04839;

CREATE TABLE t_04839
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, count(), sum(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY tuple();

-- Two parts, so the projection is merged from several pre-aggregated pieces.
INSERT INTO t_04839 SELECT number % 997, number FROM numbers(50000);
INSERT INTO t_04839 SELECT number % 997, number FROM numbers(50000, 50000);

WITH lim AS
    (
        SELECT k, count() AS c, sum(v) AS s FROM t_04839 GROUP BY k LIMIT 10
        SETTINGS optimize_trivial_group_by_limit_query = 1, optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 16
    ),
    tru AS
    (
        SELECT k, count() AS c, sum(v) AS s FROM t_04839 GROUP BY k
        SETTINGS optimize_use_projections = 1
    )
SELECT count(), countIf(lim.c != tru.c OR lim.s != tru.s) FROM lim INNER JOIN tru ON lim.k = tru.k;

DROP TABLE t_04839;
