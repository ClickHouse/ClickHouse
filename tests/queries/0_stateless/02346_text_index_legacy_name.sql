-- Index type 'inverted' was renamed to 'full_text' in April 2024.
-- Such indexes are experimental. Test what happens when ClickHouse encounters tables with the old index type.

DROP TABLE IF EXISTS tab;

-- It must be possible to load old tables with 'inverted'-type indexes
-- In stateless tests, we cannot use old persistences. Emulate "loading an old index" by creating it (internally, similar code executes).

-- Creation only works with the (old) setting enabled.
SET allow_experimental_inverted_index = 0;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE inverted(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k; -- { serverError ILLEGAL_INDEX }

SET allow_experimental_inverted_index = 1;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE inverted(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k;
INSERT INTO tab VALUES (1, 'ab') (2, 'bc');

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

-- To encourage users to migrate to the new index type, we now throw an exception when the index is used by queries.
SELECT * from tab WHERE s = 'bc'; -- { serverError ILLEGAL_INDEX }

-- The exception recommends to drop the index and create a text index instead. Let's try.
ALTER TABLE tab DROP INDEX idx;
SET allow_experimental_full_text_index = 1; -- the new setting
ALTER TABLE tab ADD INDEX idx(s) TYPE gin(tokenizer = 'ngram', ngram_size = 2);

SELECT * from tab WHERE s = 'bc';

DROP TABLE tab;

-- ----------------------------------------------------------------------------------------------------------------------------------------

-- Index type 'full_text' was renamed to 'gin' in April 2025.
-- Such indexes are experimental. Test what happens when ClickHouse encounters tables with the old index type.

-- It must be possible to load old tables with 'full_text'-type indexes
-- In stateless tests, we cannot use old persistences. Emulate "loading an old index" by creating it (internally, similar code executes).

-- Creation only works with the (old) setting enabled.
SET allow_experimental_full_text_index = 0;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE full_text(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k; -- { serverError ILLEGAL_INDEX }

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE full_text(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k;
INSERT INTO tab VALUES (1, 'ab') (2, 'bc');

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

-- To encourage users to migrate to the new index type, we now throw an exception when the index is used by queries.
SELECT * from tab WHERE s = 'bc'; -- { serverError ILLEGAL_INDEX }

-- The exception recommends to drop the index and create a text index instead. Let's try.
ALTER TABLE tab DROP INDEX idx;
ALTER TABLE tab ADD INDEX idx(s) TYPE gin(tokenizer = 'ngram', ngram_size = 2);

SELECT * from tab WHERE s = 'bc';

DROP TABLE tab;

-- ----------------------------------------------------------------------------------------------------------------------------------------

-- Index type 'gin' was renamed to 'text' in May 2025.
-- Such indexes are experimental. Test what happens when ClickHouse encounters tables with the old index type.

-- It must be possible to load old tables with 'gin'-type indexes
-- In stateless tests, we cannot use old persistences. Emulate "loading an old index" by creating it (internally, similar code executes).

-- Creation only works with the (old) setting enabled.
SET allow_experimental_full_text_index = 0;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE gin(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k SETTINGS min_bytes_for_full_part_storage = 0; -- { serverError ILLEGAL_INDEX }

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE gin(tokenizer = 'ngram', ngram_size = 2)) ENGINE = MergeTree() ORDER BY k SETTINGS min_bytes_for_full_part_storage = 0;
INSERT INTO tab VALUES (1, 'ab') (2, 'bc');

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

-- To encourage users to migrate to the new index type, we now throw an exception when the index is used by queries.
SELECT * from tab WHERE s = 'bc'; -- { serverError ILLEGAL_INDEX }

-- The exception recommends to drop the index and create a text index instead. Let's try.
ALTER TABLE tab DROP INDEX idx;
ALTER TABLE tab ADD INDEX idx(s) TYPE gin(tokenizer = 'ngram', ngram_size = 2);

SELECT * from tab WHERE s = 'bc';

DROP TABLE tab;
