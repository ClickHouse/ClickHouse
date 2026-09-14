-- Reads a Merge table whose children disagree on whether the Map has text indexes.

SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_plain;

CREATE TABLE t_idx (
    id UInt64,
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array'),
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

CREATE TABLE t_plain (
    id UInt64,
    map Map(String, String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_idx VALUES (1, map('env', 'prod'));
INSERT INTO t_plain VALUES (2, map('env', 'prod'));

SELECT '-- heterogeneous children, empty child header (the witness)';
SELECT count() FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env'];
SELECT count() FROM merge(currentDatabase(), '^t_') WHERE 'nomatch' IN map['env'];
SELECT 1 FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env'] ORDER BY 1;

SELECT '-- heterogeneous children, non-empty child header';
SELECT id FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env'] ORDER BY id;
SELECT sum(id) FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env'];

SELECT '-- only the virtual column used by the filter is read (mapKeys used, mapValues absent)';
SELECT countIf(explain LIKE '%__text_index_idx_map_keys_%') > 0, countIf(explain LIKE '%__text_index_idx_map_values_%')
FROM (EXPLAIN header = 1 SELECT count() FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env']);

SELECT '-- single table, no merge';
SELECT count() FROM t_idx WHERE 'prod' IN map['env'];

SELECT '-- all children indexed';
DROP TABLE t_plain;
CREATE TABLE t_plain (
    id UInt64,
    map Map(String, String),
    INDEX idx_map_keys mapKeys(map) TYPE text(tokenizer = 'array'),
    INDEX idx_map_values mapValues(map) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_plain VALUES (2, map('env', 'prod'));
SELECT count() FROM merge(currentDatabase(), '^t_') WHERE 'prod' IN map['env'];

DROP TABLE t_idx;
DROP TABLE t_plain;
