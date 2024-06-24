DROP TABLE IF EXISTS tab;

-- Index type 'inverted' was renamed to 'full_text' in April 2024.
-- Such indexes are experimental. Nevertheless test what happens when ClickHouse encounters tables with the old index type.

-- Create a full text index with the old type
-- This was how it was done in the old days. These days this throws an exception.
SET allow_experimental_inverted_index = 1;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE inverted(2)) ENGINE = MergeTree() ORDER BY k; -- { serverError ILLEGAL_INDEX };

-- There are unfortunately side effects of this behavior. In particular, if ClickHouse's automatic table load during
-- startup finds a table with 'inverted'-type indexes created by an older version, it immediately halts as it thinks
-- the persistence is corrupt. Similarly (but less severely), tables with 'inverted' index cannot be attached.
-- A backdoor avoids this. Just set allow_experimental_inverted_index = 0 (which is the default).
--
-- Note that the backdoor will exist only temporarily during a transition period. It will be removed in future. Its only purpose is
-- to simplify the migrationn of experimental inverted indexes to experimental full-text indexes instead of simply breaking existing
-- tables.
SET allow_experimental_inverted_index = 0;
CREATE TABLE tab(k UInt64, s String, INDEX idx(s) TYPE inverted(2)) ENGINE = MergeTree() ORDER BY k;
INSERT INTO tab VALUES (1, 'ab') (2, 'bc');

-- Detach and attach should work.
DETACH TABLE tab;
ATTACH TABLE tab;

-- No, the backdoor does not make 'inverted' indexes non-experimental.
-- On the one hand, the backdoor is undocumented, on the other hand, SELECTs that use such indexes now throw an exception,
-- making 'inverted' indexes useless.
SELECT * from tab WHERE s = 'bc'; -- { serverError ILLEGAL_INDEX }

-- The exception recommends to drop the index and create a 'full_text' index instead. Let's try.
ALTER TABLE tab DROP INDEX idx;
SET allow_experimental_full_text_index = 1; -- note that this is a different setting
ALTER TABLE tab ADD INDEX idx(s) TYPE full_text(2);

SELECT * from tab WHERE s = 'bc';

DROP TABLE tab;
