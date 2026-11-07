-- A skip index name containing '/' can only be stored with `escape_index_filenames` enabled: without
-- escaping the name becomes a part of the file name as is, so the index could not be named at all.
-- Turning the setting off for such a table must be rejected up front, instead of leaving the table in
-- a state where every read path (and `system.parts.secondary_indices_materialized`) surfaces a
-- filename-encoding exception.

DROP TABLE IF EXISTS t_slash_index;

CREATE TABLE t_slash_index
(
    k UInt64,
    v UInt64,
    INDEX `idx/v` v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 1, index_granularity = 8;

INSERT INTO t_slash_index SELECT number, number FROM numbers(100);

SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_slash_index' AND active;

ALTER TABLE t_slash_index MODIFY SETTING escape_index_filenames = 0; -- { serverError BAD_ARGUMENTS }

-- The rejected ALTER left the table untouched: the index is still materialized and still usable.
SELECT secondary_indices_materialized FROM system.parts
WHERE database = currentDatabase() AND table = 't_slash_index' AND active;

SELECT count() FROM t_slash_index WHERE v = 42;

-- Adding such an index while escaping is off is rejected as well.
DROP TABLE t_slash_index;

CREATE TABLE t_slash_index
(
    k UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY k
SETTINGS escape_index_filenames = 0;

ALTER TABLE t_slash_index ADD INDEX `idx/v` v TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_slash_index;
