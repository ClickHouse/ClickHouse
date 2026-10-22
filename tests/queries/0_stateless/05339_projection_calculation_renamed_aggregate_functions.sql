-- The Analyzer resolves an aggregate function into another one with a different name: by the types of
-- its arguments (`sum` of `NULL` is `nothingNull`), by the nulls action (`count() IGNORE NULLS` is `count`),
-- or by the canonical name of an alias (`SUM` is `sum`). A projection must still store the columns
-- under the names its metadata describes.

DROP TABLE IF EXISTS t_projection_renamed;

CREATE TABLE t_projection_renamed
(
    a UInt32,
    b String,
    PROJECTION p
    (
        SELECT b, sum(toString(NULL, isNull(a), isNull(b)), a), count() IGNORE NULLS, SUM(a), any(a) RESPECT NULLS
        GROUP BY b
    )
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_projection_renamed VALUES (1, 'x'), (2, 'x'), (3, 'y');

SELECT b, count(), sum(a) FROM t_projection_renamed GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

OPTIMIZE TABLE t_projection_renamed FINAL;

SELECT b, count(), sum(a) FROM t_projection_renamed GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_projection_renamed;
