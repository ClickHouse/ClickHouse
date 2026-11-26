-- Tags: no-parallel-replicas
-- Test: a JSONAllPaths skip index must not be applied to combined literal+sub-object subcolumns.
--
-- The combined subcolumn json.@`a` is not NULL when the path `a` has only sub-paths (`a.b`),
-- so the presence of the path `a` itself in JSONAllPaths is not an equivalent condition
-- and cannot be used for granule skipping.
SET explain_query_plan_default = 'legacy';

-- =============================================================================
-- Section 1: bloom_filter index, path with a sub-object
-- =============================================================================

DROP TABLE IF EXISTS t_json_combined_bf;
CREATE TABLE t_json_combined_bf
(
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_json_combined_bf VALUES ('{"a": {"b": 1}}'), ('{"a": {"b": 2}}');
INSERT INTO t_json_combined_bf VALUES ('{"x": 1}'), ('{"y": 2}');

SELECT 'bloom_filter isNotNull', count() FROM t_json_combined_bf WHERE isNotNull(json.`@\`a\``);
SELECT 'bloom_filter isNotNull no index', count() FROM t_json_combined_bf WHERE isNotNull(json.`@\`a\``) SETTINGS use_skip_indexes = 0;

-- The index is not applied at all, so all granules are read.
SELECT 'bloom_filter index not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_combined_bf WHERE isNotNull(json.`@\`a\``))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_combined_bf;

-- =============================================================================
-- Section 2: bloom_filter index, path with a literal value
-- =============================================================================

DROP TABLE IF EXISTS t_json_combined_literal;
CREATE TABLE t_json_combined_literal
(
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_json_combined_literal VALUES ('{"a": 1}'), ('{"a": 2}');
INSERT INTO t_json_combined_literal VALUES ('{"x": 1}'), ('{"y": 2}');

SELECT 'bloom_filter equals', count() FROM t_json_combined_literal WHERE json.`@\`a\`` = 2;
SELECT 'bloom_filter equals no index', count() FROM t_json_combined_literal WHERE json.`@\`a\`` = 2 SETTINGS use_skip_indexes = 0;

SELECT 'bloom_filter in', count() FROM t_json_combined_literal WHERE json.`@\`a\``::Int64 IN (2, 3);
SELECT 'bloom_filter in no index', count() FROM t_json_combined_literal WHERE json.`@\`a\``::Int64 IN (2, 3) SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_combined_literal;

-- =============================================================================
-- Section 3: text index
-- =============================================================================

DROP TABLE IF EXISTS t_json_combined_text;
CREATE TABLE t_json_combined_text
(
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_json_combined_text VALUES ('{"a": {"b": 1}}'), ('{"a": {"b": 2}}');
INSERT INTO t_json_combined_text VALUES ('{"x": 1}'), ('{"y": 2}');

SELECT 'text isNotNull', count() FROM t_json_combined_text WHERE isNotNull(json.`@\`a\``);
SELECT 'text isNotNull no index', count() FROM t_json_combined_text WHERE isNotNull(json.`@\`a\``) SETTINGS use_skip_indexes = 0;

SELECT 'text index not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_combined_text WHERE isNotNull(json.`@\`a\``))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_combined_text;

-- With the 'array' tokenizer every path is a single token, so a search for the mangled
-- path name @`a` matches nothing and all granules would be skipped.
DROP TABLE IF EXISTS t_json_combined_text_array;
CREATE TABLE t_json_combined_text_array
(
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_json_combined_text_array VALUES ('{"a": {"b": 1}}'), ('{"a": {"b": 2}}');
INSERT INTO t_json_combined_text_array VALUES ('{"x": 1}'), ('{"y": 2}');

SELECT 'text array isNotNull', count() FROM t_json_combined_text_array WHERE isNotNull(json.`@\`a\``);
SELECT 'text array isNotNull no index', count() FROM t_json_combined_text_array WHERE isNotNull(json.`@\`a\``) SETTINGS use_skip_indexes = 0;

SELECT 'text array index not used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_combined_text_array WHERE isNotNull(json.`@\`a\``))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_combined_text_array;

-- =============================================================================
-- Section 4: paths named @a and ^a
-- =============================================================================

-- Back-quoted paths starting with @ or ^ are ordinary paths, not the prefixed subcolumn syntax,
-- so the index is still applied to them.
DROP TABLE IF EXISTS t_json_prefix_keys;
CREATE TABLE t_json_prefix_keys
(
    json JSON,
    INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 2;

INSERT INTO t_json_prefix_keys VALUES ('{"@a": 1}'), ('{"^a": 2}');
INSERT INTO t_json_prefix_keys VALUES ('{"x": 1}'), ('{"y": 2}');

SELECT 'path named @a', count() FROM t_json_prefix_keys WHERE isNotNull(json.`@a`);
SELECT 'path named @a no index', count() FROM t_json_prefix_keys WHERE isNotNull(json.`@a`) SETTINGS use_skip_indexes = 0;

SELECT 'path named ^a', count() FROM t_json_prefix_keys WHERE isNotNull(json.`^a`);
SELECT 'path named ^a no index', count() FROM t_json_prefix_keys WHERE isNotNull(json.`^a`) SETTINGS use_skip_indexes = 0;

SELECT 'path named @a index used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_prefix_keys WHERE isNotNull(json.`@a`))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

SELECT 'path named ^a index used';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT * FROM t_json_prefix_keys WHERE isNotNull(json.`^a`))
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%Skip%';

DROP TABLE t_json_prefix_keys;
