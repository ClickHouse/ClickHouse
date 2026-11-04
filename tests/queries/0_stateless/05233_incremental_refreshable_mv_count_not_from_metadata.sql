-- Tags: atomic-database
-- An incremental refreshable materialized view over an aggregate: each refresh must count only the
-- rows committed since the previous one, not the whole source table.

-- The projection gate declines under parallel replicas, which would defeat the control at the end.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS incr_count_src;
DROP TABLE IF EXISTS incr_count_tgt;
DROP TABLE IF EXISTS incr_count_mv;

CREATE TABLE incr_count_src (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE incr_count_tgt (c UInt64) ENGINE = MergeTree ORDER BY tuple();

-- The projection settings are pinned in the view query because CI randomizes them; with the implicit
-- min-max/count projection off the refresh reads through the reader and the test observes nothing.
CREATE MATERIALIZED VIEW incr_count_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO incr_count_tgt EMPTY
    AS SELECT count() AS c FROM incr_count_src
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

INSERT INTO incr_count_src SELECT number, number * 10 FROM numbers(5);
SYSTEM REFRESH VIEW incr_count_mv;
SYSTEM WAIT VIEW incr_count_mv;
SELECT 'round1', count(), sum(c) FROM incr_count_tgt;

INSERT INTO incr_count_src SELECT number, number * 10 FROM numbers(5, 7);
SYSTEM REFRESH VIEW incr_count_mv;
SYSTEM WAIT VIEW incr_count_mv;
SELECT 'round2', count(), sum(c) FROM incr_count_tgt;

-- Nothing new committed: the refresh appends a zero, not the whole-table count again.
SYSTEM REFRESH VIEW incr_count_mv;
SYSTEM WAIT VIEW incr_count_mv;
SELECT 'round3', count(), sum(c) FROM incr_count_tgt;

-- Control: a plain count() over this table is served from part metadata, so the refreshes above
-- could have been answered the same way.
SELECT 'count from metadata is available', count() > 0
FROM (EXPLAIN SELECT count() FROM incr_count_src
      SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1)
WHERE explain ILIKE '%_minmax_count_projection%';

DROP TABLE incr_count_mv;
DROP TABLE incr_count_tgt;
DROP TABLE incr_count_src;
