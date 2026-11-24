SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

-- Both preprocessors build `arrayMap(lambda, ...)` over an Array column, so their lambdas must stay distinct when both are merged into one DAG.

CREATE TABLE tab
(
    id UInt64,
    a Array(String),
    b Array(String),
    INDEX idx_a a TYPE text(tokenizer = array, preprocessor = lower(a)),
    INDEX idx_b b TYPE text(tokenizer = array, preprocessor = upper(b))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo'], ['Bar']), (2, ['foo'], ['bar']);

SELECT 'two preprocessors in one DAG';
SELECT countIf(hasAnyTokens(a, ['foo'])), countIf(hasAnyTokens(b, ['BAR'])) FROM tab;
SELECT count() FROM tab WHERE hasAnyTokens(a, ['foo']) AND hasAnyTokens(b, ['BAR']);
SELECT count() FROM tab WHERE hasAnyTokens(a, ['foo']) AND hasAnyTokens(b, ['BAR']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(a, ['foo']) AND hasAnyTokens(b, ['BAR']) SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'preprocessor next to a query lambda';
SELECT countIf(hasAnyTokens(a, ['foo'])), any(arrayMap(x -> upper(x), a)) FROM tab;
SELECT countIf(hasAnyTokens(a, ['foo'])), any(arrayMap(x -> upper(x), a)) FROM tab;

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    a Array(String),
    b Array(String),
    INDEX idx_a a TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(a)),
    INDEX idx_b b TYPE text(tokenizer = splitByNonAlpha, preprocessor = upper(b))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['X y'], ['x Y']), (2, ['z'], ['z']);

SELECT 'string needles and a query lambda in one SELECT list';
SELECT id, hasAnyTokens(a, 'x'), hasAnyTokens(b, 'X'), arrayMap(x -> x, a) FROM tab ORDER BY id;
SELECT id, hasAnyTokens(a, 'x'), hasAnyTokens(b, 'X'), arrayMap(x -> x, a) FROM tab ORDER BY id;

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    a Array(String),
    INDEX idx_a a TYPE text(tokenizer = array, postprocessor = lower(a))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, ['Foo']), (2, ['foo']);

SELECT 'postprocessor next to a query lambda';
SELECT countIf(hasAnyTokens(a, ['foo'])), any(arrayMap(x -> upper(x), a)) FROM tab;
SELECT countIf(hasAnyTokens(a, ['foo'])), any(arrayMap(x -> upper(x), a)) FROM tab;

DROP TABLE tab;
