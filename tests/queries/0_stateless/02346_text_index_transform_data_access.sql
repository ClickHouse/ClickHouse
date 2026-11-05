-- Text index transforms are analysed under the global context, so anything that reads a table or a
-- dictionary while being analysed would do it with full access, and constant folding could reveal
-- what it read. A transform must be a pure function of the indexed value, so these are rejected
-- before any analysis. See 02346_text_index_transform_access for the function permission checks.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS set_source;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE set_source (x String) ENGINE = Set;
INSERT INTO set_source VALUES ('secret');

CREATE TABLE dict_source (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO dict_source VALUES (1, 'secret');
CREATE DICTIONARY dict (id UInt64, value String) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_source')) LAYOUT(FLAT()) LIFETIME(0);

SELECT '1. A set from a table.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val IN set_source, val, ''))
)
ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '2. A subquery.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(val, (SELECT any(value) FROM dict_source)))
)
ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '3. A dictionary lookup.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha',
        preprocessor = concat(val, toString(toUInt64(dictGetString(currentDatabase() || '.dict', 'value', toUInt64(1))))))
)
ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '4. The same via ALTER ADD INDEX.';

CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val IN set_source, val, '')); -- { serverError INCORRECT_QUERY }
ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val IN (SELECT x FROM set_source), val, '')); -- { serverError INCORRECT_QUERY }
ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha',
    preprocessor = concat(val, toString(toUInt64(dictGetString(currentDatabase() || '.dict', 'value', toUInt64(1)))))); -- { serverError INCORRECT_QUERY }

SELECT '5. A literal set is still accepted.';

ALTER TABLE tab ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(val IN ('hello', 'world'), val, ''));
INSERT INTO tab VALUES (1, 'hello there');
SELECT count() FROM tab WHERE hasAllTokens(val, ['hello']);

DROP TABLE tab;
DROP DICTIONARY dict;
DROP TABLE dict_source;
DROP TABLE set_source;
