DROP TABLE IF EXISTS t_dedup_sparse_src;
DROP TABLE IF EXISTS t_dedup_sparse_dst;
DROP TABLE IF EXISTS t_dedup_sparse_nested_src;
DROP TABLE IF EXISTS t_dedup_sparse_nested_dst;
DROP TABLE IF EXISTS t_dedup_sparse_landing;
DROP TABLE IF EXISTS t_dedup_sparse_mv_target;
DROP VIEW IF EXISTS t_dedup_sparse_mv;
DROP TABLE IF EXISTS t_dedup_sparse_top_src;
DROP TABLE IF EXISTS t_dedup_sparse_top_dst;
DROP TABLE IF EXISTS t_dedup_sparse_plain_dst;

-- The INSERT SELECT dedup token embeds how the source read was chunked, so the retry only
-- collides if both attempts chunk identically. Pin everything that shapes the read, or
-- randomized settings and read-scheduler timing flake the dedup pairs to 40000.
SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65409;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- A sparse-serialized element inside a Tuple: the tuple element is default in all but 5 of 20000 rows.
CREATE TABLE t_dedup_sparse_src (id UInt64, body Tuple(key UInt64, flag Bool))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_src SELECT number, (number, number < 5) FROM numbers(20000);

SELECT 'source element is sparse', dumpColumnStructure(body) LIKE '%Sparse%' FROM t_dedup_sparse_src LIMIT 1;

CREATE TABLE t_dedup_sparse_dst (id UInt64, body Tuple(key UInt64, flag Bool))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100, ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src SETTINGS insert_deduplication_token = 'token_1';
SELECT 'nested tuple, first insert', count() FROM t_dedup_sparse_dst;

-- Every block is a duplicate, so the deduplication retry is entered with nothing to retry.
INSERT INTO t_dedup_sparse_dst SELECT * FROM t_dedup_sparse_src SETTINGS insert_deduplication_token = 'token_1';
SELECT 'nested tuple, repeated insert deduplicated', count() FROM t_dedup_sparse_dst;

-- The retried table keeps the sparse serialization of the tuple element.
SELECT 'destination element is still sparse', dumpColumnStructure(body) LIKE '%Sparse%' FROM t_dedup_sparse_dst LIMIT 1;

-- Sparse nested two levels deep.
CREATE TABLE t_dedup_sparse_nested_src (id UInt64, body Tuple(inner Tuple(key UInt64, flag Bool)))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_nested_src SELECT number, tuple((number, number < 5)) FROM numbers(20000);

SELECT 'doubly nested source element is sparse', dumpColumnStructure(body) LIKE '%Sparse%' FROM t_dedup_sparse_nested_src LIMIT 1;

CREATE TABLE t_dedup_sparse_nested_dst (id UInt64, body Tuple(inner Tuple(key UInt64, flag Bool)))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100, ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_nested_dst SELECT * FROM t_dedup_sparse_nested_src SETTINGS insert_deduplication_token = 'token_2';
SELECT 'doubly nested tuple, first insert', count() FROM t_dedup_sparse_nested_dst;

INSERT INTO t_dedup_sparse_nested_dst SELECT * FROM t_dedup_sparse_nested_src SETTINGS insert_deduplication_token = 'token_2';
SELECT 'doubly nested tuple, repeated insert deduplicated', count() FROM t_dedup_sparse_nested_dst;

SELECT 'doubly nested destination element is still sparse', dumpColumnStructure(body) LIKE '%Sparse%' FROM t_dedup_sparse_nested_dst LIMIT 1;

-- The same, but deduplication happens on a materialized view target, which retries at view level.
CREATE TABLE t_dedup_sparse_landing (id UInt64, body Tuple(key UInt64, flag Bool))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

CREATE TABLE t_dedup_sparse_mv_target (id UInt64, body Tuple(key UInt64, flag Bool))
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100, ratio_of_defaults_for_sparse_serialization = 0.9;

CREATE MATERIALIZED VIEW t_dedup_sparse_mv TO t_dedup_sparse_mv_target
AS SELECT id, body FROM t_dedup_sparse_landing;

INSERT INTO t_dedup_sparse_landing SELECT * FROM t_dedup_sparse_src SETTINGS insert_deduplication_token = 'token_3';
SELECT 'materialized view target, first insert', count() FROM t_dedup_sparse_mv_target;

INSERT INTO t_dedup_sparse_landing SELECT * FROM t_dedup_sparse_src SETTINGS insert_deduplication_token = 'token_3';
SELECT 'materialized view target, repeated insert deduplicated', count() FROM t_dedup_sparse_mv_target;

SELECT 'materialized view target element is still sparse', dumpColumnStructure(body) LIKE '%Sparse%' FROM t_dedup_sparse_mv_target LIMIT 1;

-- A sparse column at the top level was already handled and must keep working.
CREATE TABLE t_dedup_sparse_top_src (id UInt64, flag Bool)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_top_src SELECT number, number < 5 FROM numbers(20000);

SELECT 'top level column is sparse', serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dedup_sparse_top_src' AND column = 'flag' AND active;

CREATE TABLE t_dedup_sparse_top_dst (id UInt64, flag Bool)
ENGINE = MergeTree ORDER BY id
SETTINGS non_replicated_deduplication_window = 100, ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_top_dst SELECT * FROM t_dedup_sparse_top_src SETTINGS insert_deduplication_token = 'token_4';
INSERT INTO t_dedup_sparse_top_dst SELECT * FROM t_dedup_sparse_top_src SETTINGS insert_deduplication_token = 'token_4';
SELECT 'top level sparse, repeated insert deduplicated', count() FROM t_dedup_sparse_top_dst;

-- An insert of the same sparse data without deduplication must still append.
CREATE TABLE t_dedup_sparse_plain_dst (id UInt64, body Tuple(key UInt64, flag Bool))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_dedup_sparse_plain_dst SELECT * FROM t_dedup_sparse_src;
INSERT INTO t_dedup_sparse_plain_dst SELECT * FROM t_dedup_sparse_src;
SELECT 'insert without deduplication appends', count() FROM t_dedup_sparse_plain_dst;

DROP VIEW t_dedup_sparse_mv;
DROP TABLE t_dedup_sparse_plain_dst;
DROP TABLE t_dedup_sparse_top_dst;
DROP TABLE t_dedup_sparse_top_src;
DROP TABLE t_dedup_sparse_mv_target;
DROP TABLE t_dedup_sparse_landing;
DROP TABLE t_dedup_sparse_nested_dst;
DROP TABLE t_dedup_sparse_nested_src;
DROP TABLE t_dedup_sparse_dst;
DROP TABLE t_dedup_sparse_src;
