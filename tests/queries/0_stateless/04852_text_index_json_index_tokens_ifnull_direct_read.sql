SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS json_index_tokens_ifnull;
CREATE TABLE json_index_tokens_ifnull
(
    id UInt64,
    data JSON(s Nullable(String)),
    dynamic_data JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX dynamic_tokens dynamic_data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_ifnull;
INSERT INTO json_index_tokens_ifnull VALUES
    (1, '{"s":"mcp"}', '{"value":42}'),
    (2, '{"s":"other"}', '{"value":"42"}'),
    (3, '{"s":""}', '{"value":43}'),
    (4, '{"s":null}', '{"value":null}'),
    (5, '{}', '{}');

SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') = 'mcp';
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE 'mcp' = ifNull(data.s, '');
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE coalesce(data.s, '') = 'mcp';
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') LIKE '%other%';
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') ILIKE '%OTHER%';
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE match(ifNull(data.s, ''), '^mcp$');
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(toString(dynamic_data.value), '') = '42';
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(dynamic_data.value, '') = '42';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') = 'mcp'
)
WHERE position(explain, '__text_index') > 0;

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') = 'mcp'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') LIKE '%other%'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '') ILIKE '%OTHER%'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(toString(dynamic_data.value), '') = '42'
)
WHERE explain LIKE '%INPUT%dynamic_data.value%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(dynamic_data.value, '') = '42'
)
WHERE explain LIKE '%INPUT%dynamic_data.value%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE 'mcp' = ifNull(data.s, '')
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(data.s, '%') LIKE '%';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, '%') LIKE '%'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE coalesce(data.s, '') = 'mcp'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE ifNull(data.s, 'mcp') = 'mcp';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE ifNull(data.s, 'mcp') = 'mcp'
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('mcp', 'other'));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE globalIn(coalesce(data.s, ''), tuple('mcp', 'other'));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), ['mcp', 'other']);
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(ifNull(toString(dynamic_data.value), ''), tuple('42', '43'));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE has(['mcp', 'other'], ifNull(data.s, ''));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE has(['42'], ifNull(toString(dynamic_data.value), ''));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE match(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), '^other$');
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(toString(nullIf(nullIf(ifNull(data.s, ''), ''), 'null')), ['mcp']);

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('mcp', 'other'))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE globalIn(coalesce(data.s, ''), tuple('mcp', 'other'))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE in(ifNull(toString(dynamic_data.value), ''), tuple('42', '43'))
)
WHERE explain LIKE '%INPUT%dynamic_data.value%';

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE has(['mcp', 'other'], ifNull(data.s, ''))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() > 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE match(nullIf(nullIf(ifNull(data.s, ''), ''), 'null'), '^other$')
)
WHERE explain LIKE '%Name: tokens%';

SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('', 'mcp'));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE in(nullIf(data.s, 'mcp'), tuple('mcp'));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE has([''], ifNull(data.s, ''));
SELECT groupArray(id) FROM json_index_tokens_ifnull WHERE match(nullIf(data.s, 'other'), '^other$');

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE in(ifNull(data.s, ''), tuple('', 'mcp'))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE in(nullIf(data.s, 'mcp'), tuple('mcp'))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE has([''], ifNull(data.s, ''))
)
WHERE explain LIKE '%INPUT%data.s%';

SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_index_tokens_ifnull WHERE match(nullIf(data.s, 'other'), '^other$')
)
WHERE explain LIKE '%Name: tokens%';

DROP TABLE json_index_tokens_ifnull;
