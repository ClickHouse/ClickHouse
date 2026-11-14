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

-- Chained access over typed JSON / Map subcolumns on indexed JSON column
DROP TABLE IF EXISTS t_json_chained;
CREATE TABLE t_json_chained
(
    id UInt64,
    j JSON(a JSON),
    INDEX idx_paths JSONAllPaths(j) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_chained VALUES (1, '{"a":{"b":42}}');

SELECT j['a']['b'] FROM t_json_chained SETTINGS optimize_functions_to_subcolumns = 0;
SELECT j['a']['b'] FROM t_json_chained SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_json_chained WHERE j['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_json_chained WHERE j['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_json_chained;

DROP TABLE IF EXISTS t_json_map_chained;
CREATE TABLE t_json_map_chained
(
    id UInt64,
    j JSON(a Map(String, UInt64)),
    INDEX idx_paths JSONAllPaths(j) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_map_chained VALUES (1, '{"a":{"b":42}}');

SELECT j['a']['b'] FROM t_json_map_chained SETTINGS optimize_functions_to_subcolumns = 0;
SELECT j['a']['b'] FROM t_json_map_chained SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_json_map_chained WHERE j['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_json_map_chained WHERE j['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_json_map_chained;

-- Secondary index pruning survives when j['k'] defines the index
DROP TABLE IF EXISTS t_json_idx_pruning;
CREATE TABLE t_json_idx_pruning
(
    id UInt64,
    j JSON(k UInt64),
    INDEX idx_k j['k'] TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_json_idx_pruning SELECT number, concat('{"k":', toString(number), '}')::JSON FROM numbers(100);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json_idx_pruning WHERE j['k'] = 42) WHERE explain LIKE '%idx_k%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json_idx_pruning WHERE j['k'] = 42) WHERE explain LIKE '%1/100%';

DROP TABLE t_json_idx_pruning;

-- Primary key pruning survives when j['k'] defines the primary key
DROP TABLE IF EXISTS t_json_pk_pruning;
CREATE TABLE t_json_pk_pruning
(
    id UInt64,
    j JSON(k UInt64)
)
ENGINE = MergeTree ORDER BY j['k']
SETTINGS index_granularity = 1;

INSERT INTO t_json_pk_pruning SELECT number, concat('{"k":', toString(number), '}')::JSON FROM numbers(100);

SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_json_pk_pruning WHERE j['k'] = 42) WHERE explain LIKE '%Condition:%' AND NOT (explain LIKE '%Condition: true%');

DROP TABLE t_json_pk_pruning;

