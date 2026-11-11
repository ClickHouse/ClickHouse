-- An `ALTER` that adds an index is authorized against the schema the whole statement produces.
-- That must not skip the validation of its other commands and run into an exception.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

ALTER TABLE tab
    RENAME COLUMN missing TO renamed,
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

ALTER TABLE tab
    RENAME COLUMN val TO a,
    RENAME COLUMN a TO b,
    ADD INDEX idx(id) TYPE minmax GRANULARITY 1; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE tab
    RENAME COLUMN IF EXISTS missing TO renamed,
    ADD INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val)) GRANULARITY 1;

SELECT name, type FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

DROP TABLE tab;
