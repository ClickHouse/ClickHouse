-- The virtual columns `row_id` and `positions` of `mergeTreeTextIndex` return the posting lists
-- of the tokens, a row per posting. They must match the tokens of the rows of the table for all kinds of posting lists:
-- embedded, raw, compressed and split into several segments (whose bounds do not match the blocks of the positions).

DROP TABLE IF EXISTS tab_postings;
DROP TABLE IF EXISTS tab_postings_plain;

CREATE TABLE tab_postings
(
    id UInt32,
    s String,
    INDEX idx_s(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_frontcoding_compression = 1, posting_list_codec = 'bitpacking', posting_list_block_size = 300, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;

SYSTEM STOP MERGES tab_postings;

-- `a`: every row, 1-3 times; `b`: every 7th row; `c`: 10 rows, raw postings; `d`: 3 rows, embedded postings; `u<n>`: a token per row.
INSERT INTO tab_postings SELECT number, concat(repeat('a ', number % 3 + 1), if(number % 7 = 0, 'b ', ''), if(number < 10, 'c x c ', ''), if(number < 3, 'd d ', ''), 'u', toString(number)) FROM numbers(1000);
INSERT INTO tab_postings SELECT 1000 + number, concat('b a ', if(number < 2, 'd ', ''), 'v', toString(number)) FROM numbers(500);

DROP VIEW IF EXISTS expected_postings;
CREATE VIEW expected_postings AS
SELECT _part AS part_name, token, toUInt32(_part_offset) AS row_id, token_positions AS positions
FROM
(
    SELECT _part, _part_offset, tokens(s, 'splitByNonAlpha') AS t, arrayJoin(arrayDistinct(t)) AS token,
        arrayMap(i -> toUInt32(i - 1), arrayFilter(i -> t[i] = token, arrayEnumerate(t))) AS token_positions
    FROM tab_postings
);

SELECT '-- without the columns of the posting lists, a row per token';
SELECT * FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) WHERE token = 'd' ORDER BY part_name;
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s);

SELECT '-- embedded and raw postings';
SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) WHERE token IN ('c', 'd') ORDER BY part_name, token, row_id;

SELECT '-- compressed postings in several segments';
SELECT part_name, token, count(), min(row_id), max(row_id), sum(length(positions))
FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s)
WHERE token IN ('a', 'b')
GROUP BY part_name, token ORDER BY part_name, token;

SELECT '-- all postings match the rows';
SELECT count(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s);
SELECT sum(cardinality) FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s);
SELECT count() FROM expected_postings;
SELECT count() FROM (SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) EXCEPT SELECT * FROM expected_postings);
SELECT count() FROM (SELECT * FROM expected_postings EXCEPT SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s));

SELECT '-- a block smaller than a segment';
SELECT count() FROM (SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) EXCEPT SELECT * FROM expected_postings) SETTINGS max_block_size = 7;
-- A chunk exceeds `max_block_size` by at most one segment.
SELECT count(row_id), max(rows) FROM (SELECT blockSize() AS rows, row_id FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) WHERE token = 'a') SETTINGS max_block_size = 7;

SELECT '-- only some of the columns';
SELECT count(), sum(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) WHERE token = 'b';
SELECT count(), sum(length(positions)) FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) WHERE token = 'a';

SELECT '-- after a merge';
SYSTEM START MERGES tab_postings;
OPTIMIZE TABLE tab_postings FINAL;
SELECT count(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s);
SELECT count() FROM (SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s) EXCEPT SELECT * FROM expected_postings);
SELECT count() FROM (SELECT * FROM expected_postings EXCEPT SELECT part_name, token, row_id, positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings, idx_s));

SELECT '-- an index without positions';
CREATE TABLE tab_postings_plain
(
    id UInt32,
    s String,
    INDEX idx_s(s) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_postings_plain VALUES (0, 'raft log'), (1, 'raft raft'), (2, 'log');

SELECT token, groupArray(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_postings_plain, idx_s) GROUP BY token ORDER BY token;
SELECT positions FROM mergeTreeTextIndex(currentDatabase(), tab_postings_plain, idx_s); -- { serverError UNKNOWN_IDENTIFIER }

DROP VIEW expected_postings;
DROP TABLE tab_postings;
DROP TABLE tab_postings_plain;
