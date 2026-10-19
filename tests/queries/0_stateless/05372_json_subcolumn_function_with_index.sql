-- Tags: no-fasttest

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_json_idx;

CREATE TABLE t_json_idx
(
    id UInt64,
    j JSON,
    INDEX idx_paths JSONAllPaths(j) TYPE text(tokenizer = array)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_idx SELECT number, concat('{"k":"', toString(number), '","blob":"', repeat('x', 50), '"}')::JSON FROM numbers(100);

-- With index on JSONAllPaths(j), j['k'] should rewrite to j.@k
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_json_idx WHERE j['k'] = '42') WHERE explain LIKE '%j.@`k`%' OR explain LIKE '%j.@k%';
SELECT count() = 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_json_idx WHERE j['k'] = '42') WHERE explain LIKE '%arrayElement%';

-- Results must match dot notation
SELECT count() FROM t_json_idx WHERE j['k'] = '42';
SELECT count() FROM t_json_idx WHERE j.k = '42';

-- Filter rewrite should still apply when full column is selected
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT j FROM t_json_idx WHERE j['k'] = '42') WHERE explain LIKE '%j.@`k`%' OR explain LIKE '%j.@k%';

DROP TABLE t_json_idx;
