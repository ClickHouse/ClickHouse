SET optimize_constant_columns_after_filter = 1;

DROP TABLE IF EXISTS constant_column_after_filter_write;
DROP TABLE IF EXISTS constant_column_after_filter_projection_write;

CREATE TABLE constant_column_after_filter_write
(
    event_type String,
    message String
)
ENGINE = MergeTree
ORDER BY message
SETTINGS min_bytes_for_wide_part = 1000000000;

INSERT INTO constant_column_after_filter_write
SELECT event_type, message
FROM values('event_type String, message String', ('pageview', 'a'), ('click', 'b'), ('pageview', 'c'))
WHERE event_type = 'pageview';

SELECT 'insert-select';
SELECT event_type, message
FROM constant_column_after_filter_write
ORDER BY message;

CREATE TABLE constant_column_after_filter_projection_write
(
    time DateTime,
    event_type String,
    message String
)
ENGINE = MergeTree
-- Order the base table by a different column than the projection: since
-- https://github.com/ClickHouse/ClickHouse/pull/116854 `read-in-order` on the base table runs
-- before projection selection, so a base table sorted by `time` would serve `ORDER BY time` itself
-- and the projection would never be picked.
ORDER BY message
SETTINGS min_bytes_for_wide_part = 1000000000;

ALTER TABLE constant_column_after_filter_projection_write ADD PROJECTION pageview_projection
(
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

INSERT INTO constant_column_after_filter_projection_write VALUES
    ('2024-01-01 00:00:00', 'pageview', 'a'),
    ('2024-01-02 00:00:00', 'click', 'b'),
    ('2024-01-03 00:00:00', 'pageview', 'c');

SELECT 'projection';
SELECT event_type, time, message
FROM constant_column_after_filter_projection_write
WHERE event_type = 'pageview'
ORDER BY time
SETTINGS force_optimize_projection = 1;

SELECT sum(rows)
FROM system.projection_parts
WHERE database = currentDatabase()
    AND table = 'constant_column_after_filter_projection_write'
    AND name = 'pageview_projection'
    AND active;

DROP TABLE constant_column_after_filter_write;
DROP TABLE constant_column_after_filter_projection_write;

DROP TABLE IF EXISTS t_proj_tuple_eq;

CREATE TABLE t_proj_tuple_eq
(
    k UInt64,
    tup Tuple(UInt64, UInt64),
    PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9375;

INSERT INTO t_proj_tuple_eq VALUES (1, (1, 2)), (2, (3, 4)), (3, (1, 2));

ALTER TABLE t_proj_tuple_eq CLEAR PROJECTION p SETTINGS mutations_sync = 1;
ALTER TABLE t_proj_tuple_eq MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq' AND name = 'p' AND active;

SELECT k, tup FROM t_proj_tuple_eq ORDER BY k;
SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq;

-- The same projection calculated by a merge of parts which do not have it yet.
DROP TABLE IF EXISTS t_proj_tuple_eq_merge;

CREATE TABLE t_proj_tuple_eq_merge
(
    k UInt64,
    tup Tuple(UInt64, UInt64),
    PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS materialize_projections_on_insert = 0, materialize_projections_on_merge = 1;

INSERT INTO t_proj_tuple_eq_merge VALUES (1, (1, 2)), (2, (3, 4));
INSERT INTO t_proj_tuple_eq_merge VALUES (3, (1, 2)), (4, (3, 4));

OPTIMIZE TABLE t_proj_tuple_eq_merge FINAL;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq_merge' AND name = 'p' AND active;

SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq_merge, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq_merge;

-- The same projection added to a table with existing data and calculated by a mutation.
DROP TABLE IF EXISTS t_proj_tuple_eq_mutation;

CREATE TABLE t_proj_tuple_eq_mutation
(
    k UInt64,
    tup Tuple(UInt64, UInt64)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_proj_tuple_eq_mutation VALUES (1, (1, 2)), (2, (3, 4)), (3, (1, 2));

ALTER TABLE t_proj_tuple_eq_mutation ADD PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k);
ALTER TABLE t_proj_tuple_eq_mutation MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq_mutation' AND name = 'p' AND active;

SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq_mutation, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq_mutation;
