-- JSON column is no longer allowed in minmax index by default (see #106088).
-- Previously this test verified skip-index filtering; now it verifies rejection.

DROP TABLE IF EXISTS t_json_minmax_idx;

CREATE TABLE t_json_minmax_idx (id UInt32, j JSON, INDEX idx_j j TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity=1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_json_minmax_idx;
