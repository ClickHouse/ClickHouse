SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

-- Regression test for the empty-match fast path leaving the posting stream unaligned.
--
-- Previously, when a mark's term column produced no matches for the query, the reader
-- skipped `readData` entirely and never advanced the posting stream. On the next
-- adjacent mark (`continue_reading = true`), the posting stream still pointed at the
-- previous mark's start byte and the deserializer read postings from the wrong offset,
-- silently returning wrong results.
--
-- This test forces multiple adjacent dictionary marks where one mark has no matching
-- token and the next mark does, then verifies the result is correct.

DROP TABLE IF EXISTS t_empty_match_sync;

CREATE TABLE t_empty_match_sync
(
    id UInt32,
    msg String,
    PROJECTION idx INDEX msg TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 4)
)
ENGINE = MergeTree ORDER BY id;

-- Eight rows producing 8 distinct tokens. With dictionary_block_size = 4 the projection
-- has multiple adjacent marks.
INSERT INTO t_empty_match_sync VALUES
    (1, 'alpha'),
    (2, 'bravo'),
    (3, 'charlie'),
    (4, 'delta'),
    (5, 'echo'),
    (6, 'foxtrot'),
    (7, 'golf'),
    (8, 'hotel');

-- Query for a token in the LAST mark — the earlier marks have no matches and must
-- advance the stream correctly so the last mark is read from the right offset.
SELECT count() FROM t_empty_match_sync WHERE hasToken(msg, 'hotel');

-- Cross-mark query: two tokens in different marks.
SELECT count() FROM t_empty_match_sync WHERE hasAnyTokens(msg, ['alpha', 'hotel']);

-- A token that does not exist at all (all marks empty match).
SELECT count() FROM t_empty_match_sync WHERE hasToken(msg, 'no_such_token');

-- After the empty-match no-result query, a follow-up query that DOES match the first
-- mark must work — confirms the stream state is consistent across queries.
SELECT count() FROM t_empty_match_sync WHERE hasToken(msg, 'alpha');

DROP TABLE t_empty_match_sync;
