-- A text index must be rebuilt or reject an `ALTER` when matcher re-expansion changes
-- its normalized `preprocessor` or `postprocessor`, even if the indexed expression is unchanged.
--
-- The tables below pin `min_bytes_for_wide_part` so that the parts stay compact: a mutation that
-- only rebuilds an index does not take effect on a wide part (the existing index files are carried
-- over unchanged), which is a separate, index-type-independent gap - it reproduces for `minmax` and
-- without any matcher, too - and is reported for the mutation code, not for matcher expansion.

SET alter_sync = 2;
SET mutations_sync = 2;

SELECT '-- preprocessor change rebuilds the index';

DROP TABLE IF EXISTS text_index_preprocessor_matcher_alter;

CREATE TABLE text_index_preprocessor_matcher_alter
(
    id UInt64,
    msg String,
    prep String ALIAS concat(COLUMNS('^(msg|content|suffix)$')),
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = prep)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS alter_column_secondary_index_mode = 'rebuild', min_bytes_for_wide_part = 1000000000;

INSERT INTO text_index_preprocessor_matcher_alter VALUES (1, 'hello'), (2, 'world');

-- The normalized preprocessor changes from `concat(msg)` to `concat(msg, 'x')`.
-- The new column is an `ALIAS`, so the transform still depends only on the indexed input.
ALTER TABLE text_index_preprocessor_matcher_alter ADD COLUMN suffix String ALIAS 'x';

SELECT count()
FROM system.mutations
WHERE database = currentDatabase()
    AND table = 'text_index_preprocessor_matcher_alter'
    AND command ILIKE '%MATERIALIZE INDEX idx%';

-- Without the rebuild, the old dictionary contains `hello` while the new preprocessor
-- looks up `hellox`, so the stale index incorrectly prunes the part.
SELECT count()
FROM text_index_preprocessor_matcher_alter
WHERE hasToken(msg, 'hello')
SETTINGS force_data_skipping_indices = 'idx';

-- A pure rename changes identifiers on both sides and must not queue another rebuild.
ALTER TABLE text_index_preprocessor_matcher_alter RENAME COLUMN msg TO content;

SELECT count()
FROM system.mutations
WHERE database = currentDatabase()
    AND table = 'text_index_preprocessor_matcher_alter'
    AND command ILIKE '%MATERIALIZE INDEX idx%';

SELECT count()
FROM text_index_preprocessor_matcher_alter
WHERE hasToken(content, 'hello')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE text_index_preprocessor_matcher_alter;

SELECT '-- postprocessor change is rejected in throw mode';

DROP TABLE IF EXISTS text_index_postprocessor_matcher_alter;

CREATE TABLE text_index_postprocessor_matcher_alter
(
    id UInt64,
    msg String,
    post String ALIAS concat(COLUMNS('^(msg|suffix)$')),
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = post)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS alter_column_secondary_index_mode = 'throw', min_bytes_for_wide_part = 1000000000;

INSERT INTO text_index_postprocessor_matcher_alter VALUES (1, 'hello');

ALTER TABLE text_index_postprocessor_matcher_alter
    ADD COLUMN suffix String ALIAS 'x'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count()
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'text_index_postprocessor_matcher_alter'
    AND name = 'suffix';

SELECT count()
FROM text_index_postprocessor_matcher_alter
WHERE hasToken(msg, 'hello')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE text_index_postprocessor_matcher_alter;
