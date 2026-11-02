-- A skip index over a `MATERIALIZED` column that the column TTL makes the mutation recompute has to
-- describe the value the part ends up with. The mutation pipeline materializes the index expressions
-- into the block and the writer reuses whatever it finds there, so evaluating them before the TTL
-- wrote an index describing the pre-expiry value - and the index then prunes granules that do match.

SET materialize_ttl_after_modify = 0;

DROP TABLE IF EXISTS t_materialize_ttl_skip_index_wide;
CREATE TABLE t_materialize_ttl_skip_index_wide
(
    d DateTime,
    x Int32,
    m Int32 MATERIALIZED x + 1,
    INDEX i m * 2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_skip_index_wide (d, x) VALUES ('2000-01-01 00:00:00', 41);

ALTER TABLE t_materialize_ttl_skip_index_wide
    MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_skip_index_wide MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT x, m FROM t_materialize_ttl_skip_index_wide;
SELECT count() FROM t_materialize_ttl_skip_index_wide WHERE m * 2 = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_materialize_ttl_skip_index_wide WHERE m * 2 = 2
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';

DROP TABLE t_materialize_ttl_skip_index_wide;

-- A compact part takes the other mutation path, which rewrites every column.
DROP TABLE IF EXISTS t_materialize_ttl_skip_index_compact;
CREATE TABLE t_materialize_ttl_skip_index_compact
(
    d DateTime,
    x Int32,
    m Int32 MATERIALIZED x + 1,
    INDEX i m * 2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_skip_index_compact (d, x) VALUES ('2000-01-01 00:00:00', 41);

ALTER TABLE t_materialize_ttl_skip_index_compact
    MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_skip_index_compact MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT x, m FROM t_materialize_ttl_skip_index_compact;
SELECT count() FROM t_materialize_ttl_skip_index_compact WHERE m * 2 = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_materialize_ttl_skip_index_compact WHERE m * 2 = 2
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'i';

DROP TABLE t_materialize_ttl_skip_index_compact;
