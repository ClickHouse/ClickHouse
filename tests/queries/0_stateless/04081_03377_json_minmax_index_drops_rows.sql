-- Test that MinMax index on JSON column is rejected (JSON values can have varying types per row).
-- Related to: https://github.com/ClickHouse/ClickHouse/issues/101700

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_json_minmax_idx;

-- Creating a MinMax index on a JSON column must be rejected
CREATE TABLE t_json_minmax_idx (id UInt32, j JSON, INDEX idx_j j TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Verify JSON comparison works correctly without index
DROP TABLE IF EXISTS t_json_extremes;
CREATE TABLE t_json_extremes (id UInt32, j JSON) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_json_extremes VALUES (1, '{"a":"1"}'), (2, '{"a":"2"}'), (3, '{"a":"3"}');

SELECT id FROM t_json_extremes WHERE j > '{"a":"2"}'::JSON ORDER BY id;
SELECT id FROM t_json_extremes WHERE j = '{"a":"2"}'::JSON ORDER BY id;
SELECT id FROM t_json_extremes WHERE j < '{"a":"2"}'::JSON ORDER BY id;

DROP TABLE IF EXISTS t_json_extremes;
DROP TABLE IF EXISTS t_json_minmax_idx;
