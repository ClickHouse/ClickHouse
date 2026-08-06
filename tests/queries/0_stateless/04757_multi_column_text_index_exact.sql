SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS multi_text_exact;
DROP TABLE IF EXISTS tagged_single;

-- The field-tagged layout is experimental and must be explicitly enabled when it is created.
CREATE TABLE multi_text_setting_disabled
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(
        tokenizer = 'splitByNonAlpha',
        field_ids = '{"a":1,"b":2}')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE multi_text_exact
(
    id UInt64,
    title String,
    body String,
    tags Array(Nullable(String)),
    note LowCardinality(Nullable(String)),
    code FixedString(12),
    INDEX idx (title, body, tags, note, code) TYPE text(
        tokenizer = 'splitByNonAlpha',
        support_phrase_search = 1,
        field_ids = '{"title":7,"body":42,"tags":65535,"note":100,"code":101,"retired":99}')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    allow_experimental_multi_column_text_index = 1,
    allow_experimental_text_index_phrase_search = 1,
    max_bytes_to_merge_at_max_space_in_pool = 0;

INSERT INTO multi_text_exact VALUES
    (1, 'shared titleonly', 'bodyone', ['tagone', 'shared', NULL], NULL, 'fixedone'),
    (2, 'second', 'shared bodyonly', ['tagtwo'], 'nullableonly', 'shared'),
    (3, 'shared', 'shared', ['shared', 'tagthree'], 'shared', 'fixedthree');

INSERT INTO multi_text_exact VALUES
    (4, '', 'bodyone', [], NULL, ''),
    (5, 'prefix', 'other', ['foo bar'], 'other', 'fixedfive');

SELECT 'title_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasToken(title, 'shared') ORDER BY id);
SELECT 'body_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasToken(body, 'shared') ORDER BY id);
SELECT 'tags_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE has(tags, 'shared') ORDER BY id);
SELECT 'nullable_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasToken(note, 'shared') ORDER BY id);
SELECT 'fixed_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasAnyTokens(code, ['shared']) ORDER BY id);
SELECT 'field_isolation', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasToken(title, 'bodyonly') ORDER BY id);

-- `EXPLAIN actions = 1` must expose two distinct text-index virtual inputs for predicates on
-- different fields of the same index. Count their hashes instead of depending on plan formatting.
SELECT 'direct_read_actions', uniqExact(field_hash)
FROM
(
    SELECT arrayJoin(extractAll(explain, '__text_index_idx_hasToken_([0-9a-f]{32})')) AS field_hash
    FROM
    (
        EXPLAIN actions = 1
        SELECT id
        FROM multi_text_exact
        WHERE hasToken(title, 'titleonly') AND hasToken(body, 'bodyone')
    )
);

-- Disable direct read for this plan so `EXPLAIN indexes = 1` reports the actual skip-index
-- pruning performed by the multi-column `Text` index, including the field-tagged condition.
SELECT 'index_pruning';
SELECT trimLeft(explain)
FROM
(
    EXPLAIN indexes = 1
    SELECT id
    FROM multi_text_exact
    WHERE hasToken(body, 'bodyonly')
    SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes_on_data_read = 0
)
WHERE explain LIKE '%Name:%'
    OR explain LIKE '%Description:%'
    OR explain LIKE '%Condition:%'
    OR explain LIKE '%Parts:%/%'
    OR explain LIKE '%Granules:%/%';
SELECT 'cross_field_and', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasToken(title, 'titleonly') AND hasToken(body, 'bodyone')
    ORDER BY id
);
SELECT 'same_token_and', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasToken(title, 'shared') AND hasToken(body, 'shared')
    ORDER BY id
);
SELECT 'cross_field_or', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasToken(title, 'titleonly') OR hasToken(body, 'bodyonly')
    ORDER BY id
);
SELECT 'has_any', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasAnyTokens(title, ['second', 'shared'])
    ORDER BY id
);
SELECT 'has_all', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasAllTokens(title, ['shared', 'titleonly'])
    ORDER BY id
);
SELECT 'prepared_in', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE title IN ('second', 'shared')
    ORDER BY id
);
SELECT 'phrase', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasPhrase(title, 'shared titleonly')
    ORDER BY id
);

-- Dictionary pattern scans are a later stage. The tagged index must fall back to the row-level
-- `LIKE` predicate instead of matching its binary physical keys.
SELECT 'pattern_fallback', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE title LIKE '%shared%'
    ORDER BY id
);

SELECT 'direct_read_parity',
    (SELECT groupArray(id) FROM
    (
        SELECT id
        FROM multi_text_exact
        WHERE hasToken(body, 'shared')
        ORDER BY id
        SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes_on_data_read = 1
    )) =
    (SELECT groupArray(id) FROM
    (
        SELECT id
        FROM multi_text_exact
        WHERE hasToken(body, 'shared')
        ORDER BY id
        SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes_on_data_read = 0
    ));

OPTIMIZE TABLE multi_text_exact FINAL;

SELECT 'merged_title_shared', groupArray(id) FROM (SELECT id FROM multi_text_exact WHERE hasToken(title, 'shared') ORDER BY id);
SELECT 'merged_phrase', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_exact
    WHERE hasPhrase(title, 'shared titleonly')
    ORDER BY id
);

SELECT * FROM mergeTreeTextIndex(currentDatabase(), multi_text_exact, idx); -- { serverError BAD_ARGUMENTS }

DROP TABLE multi_text_exact;

CREATE TABLE tagged_single
(
    id UInt64,
    value String,
    INDEX idx value TYPE text(
        tokenizer = 'splitByNonAlpha',
        field_ids = '{"value":17,"retired":3}')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_multi_column_text_index = 1;

INSERT INTO tagged_single VALUES (1, 'single tagged'), (2, 'other');
SELECT 'tagged_single', groupArray(id) FROM
(
    SELECT id
    FROM tagged_single
    WHERE hasToken(value, 'tagged')
    ORDER BY id
);
DROP TABLE tagged_single;

-- A part created before `ADD INDEX` has no index files. Direct read must evaluate the source
-- predicate for that part while using the tagged index for subsequently inserted parts.
DROP TABLE IF EXISTS multi_text_materialize;
CREATE TABLE multi_text_materialize
(
    id UInt64,
    title String,
    body String
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY id;

INSERT INTO multi_text_materialize VALUES (1, 'old title', 'oldbody');
ALTER TABLE multi_text_materialize ADD INDEX idx (title, body) TYPE text(
    tokenizer = 'splitByNonAlpha',
    field_ids = '{"title":11,"body":12}'); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE multi_text_materialize MODIFY SETTING allow_experimental_multi_column_text_index = 1;
ALTER TABLE multi_text_materialize ADD INDEX idx (title, body) TYPE text(
    tokenizer = 'splitByNonAlpha',
    field_ids = '{"title":11,"body":12}');
INSERT INTO multi_text_materialize VALUES (2, 'new title', 'newbody');

SELECT 'partially_materialized', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_materialize
    WHERE hasAnyTokens(body, ['oldbody', 'newbody'])
    ORDER BY id
);

ALTER TABLE multi_text_materialize MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT 'materialized', groupArray(id) FROM
(
    SELECT id
    FROM multi_text_materialize
    WHERE hasAnyTokens(body, ['oldbody', 'newbody'])
    ORDER BY id
);
DROP TABLE multi_text_materialize;

-- `field_ids` is a user-maintained ownership table and is mandatory for multiple fields.
CREATE TABLE multi_text_missing_map
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_missing_field
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"a":1}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_duplicate_id
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"a":1,"b":2,"retired":1}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_invalid_id
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"a":0,"b":2}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_non_integer_id
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"a":"1","b":2}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

-- Transform templates are added in later stages; the first tagged stage rejects them explicitly.
CREATE TABLE multi_text_preprocessor
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(a),
        field_ids = '{"a":1,"b":2}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_postprocessor
(
    a String,
    b String,
    INDEX idx (a, b) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = lower(a),
        field_ids = '{"a":1,"b":2}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tagged_single_preprocessor
(
    a String,
    INDEX idx a TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = lower(a),
        field_ids = '{"a":1}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_expression
(
    a String,
    b String,
    INDEX idx (a, lower(b)) TYPE text(
        tokenizer = 'splitByNonAlpha',
        field_ids = '{"a":1,"lower(b)":2}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE multi_text_overlap
(
    a String,
    b String,
    c String,
    INDEX idx_ab (a, b) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"a":1,"b":2}'),
    INDEX idx_bc (b, c) TYPE text(tokenizer = 'splitByNonAlpha', field_ids = '{"b":3,"c":4}')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS allow_experimental_multi_column_text_index = 1; -- { serverError BAD_ARGUMENTS }
