-- Tags: no-parallel-replicas

-- The text index direct-read optimization aborted the server when the text-search predicate was
-- rewritten in the WHERE filter step and the indexed value is a derived expression: the filter DAG
-- input is named after that expression (`arrayMap(lambda(tuple(x), concat('-', x)), arr)`,
-- `mapValues(attributes)`), which is not a physical read column, so pruning it from the reading step's
-- read set called `vector::erase` with `end()`.
--
-- The PREWHERE predicate must be one the direct read declines (a `toLowCardinality` needle), so that
-- the rewrite falls through to the WHERE filter step, where the derived name is a DAG input.

SET enable_full_text_index = 1;
-- Only the old analyzer names the filter DAG input after the derived expression.
SET enable_analyzer = 0;
SET query_plan_direct_read_from_text_index = 1;
-- The old analyzer cannot resolve an ALIAS column in PREWHERE without alias substitution.
SET optimize_respect_aliases = 1;

SELECT 'Text index on an ALIAS column';
DROP TABLE IF EXISTS t_text_index_alias;

CREATE TABLE t_text_index_alias
(
    id UInt64,
    arr Array(String),
    arr_prefixed Array(String) ALIAS arrayMap(x -> concat('-', x), arr),
    INDEX idx_prefixed arr_prefixed TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index_alias VALUES (1, ['hello', 'world']), (2, ['foo', 'bar']);

SELECT 'No matching token';
SELECT count() FROM t_text_index_alias PREWHERE has(arr_prefixed, toLowCardinality('zzz')) WHERE has(arr_prefixed, '-hello');
SELECT 'PREWHERE and WHERE both keep row 1';
SELECT id FROM t_text_index_alias PREWHERE has(arr_prefixed, toLowCardinality('-hello')) WHERE has(arr_prefixed, '-world') ORDER BY id;
SELECT 'WHERE keeps nothing PREWHERE kept';
SELECT count() FROM t_text_index_alias PREWHERE has(arr_prefixed, toLowCardinality('-hello')) WHERE has(arr_prefixed, '-bar');

SELECT 'Direct read on';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM t_text_index_alias PREWHERE has(arr_prefixed, toLowCardinality('zzz')) WHERE has(arr_prefixed, '-hello')
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_idx_prefixed%';

SELECT 'Direct read off';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM t_text_index_alias PREWHERE has(arr_prefixed, toLowCardinality('zzz')) WHERE has(arr_prefixed, '-hello')
    SETTINGS query_plan_direct_read_from_text_index = 0
)
WHERE explain ILIKE '%__text_index_idx_prefixed%';

SELECT 'Text index on an expression over a physical column';
DROP TABLE IF EXISTS t_text_index_expression;

CREATE TABLE t_text_index_expression
(
    id UInt64,
    attributes Map(String, String),
    INDEX idx_vals mapValues(attributes) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index_expression VALUES (1, {'ip': '192.168.1.1'}), (2, {'ip': '10.0.0.1'});

SELECT 'No matching token';
SELECT count() FROM t_text_index_expression PREWHERE has(mapValues(attributes), toLowCardinality('zzz')) WHERE has(mapValues(attributes), '192.168.1.1');
SELECT 'Matching token';
SELECT id FROM t_text_index_expression PREWHERE has(mapValues(attributes), toLowCardinality('192.168.1.1')) WHERE has(mapValues(attributes), '192.168.1.1') ORDER BY id;

SELECT 'Index still usable';
SELECT id FROM t_text_index_expression PREWHERE has(mapValues(attributes), toLowCardinality('192.168.1.1')) WHERE has(mapValues(attributes), '192.168.1.1')
SETTINGS force_data_skipping_indices = 'idx_vals';

SELECT 'Direct read on';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM t_text_index_expression PREWHERE has(mapValues(attributes), toLowCardinality('zzz')) WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_idx_vals%';

SELECT 'Direct read off';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT id FROM t_text_index_expression PREWHERE has(mapValues(attributes), toLowCardinality('zzz')) WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 0
)
WHERE explain ILIKE '%__text_index_idx_vals%';

DROP TABLE t_text_index_expression;
DROP TABLE t_text_index_alias;
