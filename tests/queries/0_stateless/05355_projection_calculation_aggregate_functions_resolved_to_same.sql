-- Expressions spelled differently in a projection may be resolved by the Analyzer to the same one
-- (`count()` and `count() IGNORE NULLS`, `countDistinct(a)` and `uniqExact(a)`): the pipeline calculates
-- it once, while the projection metadata describes a separate column for every spelling.

DROP TABLE IF EXISTS t_projection_same_aggregate;

CREATE TABLE t_projection_same_aggregate
(
    a Nullable(UInt64),
    b UInt64,
    PROJECTION p
    (
        SELECT b, count(), count() IGNORE NULLS, countDistinct(a), uniqExact(a)
        GROUP BY b
    )
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_projection_same_aggregate SELECT number, number % 3 FROM numbers(10);

SELECT b, count(), uniqExact(a) FROM t_projection_same_aggregate GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_same_aggregate' AND active
ORDER BY column_position;

OPTIMIZE TABLE t_projection_same_aggregate FINAL;

SELECT b, count(), uniqExact(a) FROM t_projection_same_aggregate GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_projection_same_aggregate;
