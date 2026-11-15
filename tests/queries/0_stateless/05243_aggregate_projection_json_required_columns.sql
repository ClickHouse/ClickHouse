DROP TABLE IF EXISTS t_projection_json_required_columns;

CREATE TABLE t_projection_json_required_columns
(
    id UInt64,
    j JSON(a UInt64, b UInt64),
    v UInt64,
    PROJECTION p (SELECT id, sum(v) GROUP BY id)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_projection_json_required_columns VALUES
    (1, '{"a":1}', 10), (1, '{"a":2}', 20), (2, '{"a":3}', 30);

SELECT count() > 0
FROM (EXPLAIN SELECT id, sum(v) FROM t_projection_json_required_columns GROUP BY id
      SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p')
WHERE explain ILIKE '%ReadFromMergeTree (p)%';

SELECT id, sum(v) FROM t_projection_json_required_columns GROUP BY id ORDER BY id
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_json_required_columns;
