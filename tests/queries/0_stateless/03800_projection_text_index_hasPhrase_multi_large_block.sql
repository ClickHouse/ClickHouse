SET allow_experimental_projection_text_index = 1;

-- Test hasPhrase correctness when a token's posting list spans multiple large blocks.
-- Uses a small posting_list_block_size (512) so that ~2000 docs per token produces 4+ large blocks.
-- This is a regression test for the pos_start_offset bug: without per-large-block .pos offsets,
-- position seeks for the 2nd+ large block would land at offset 0 in .pos (the first large block's data).

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_phrase_multi_lb;
CREATE TABLE t_phrase_multi_lb (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        enable_phrase_query_support = 1,
        posting_list_block_size = 512)
) ENGINE = MergeTree ORDER BY id;

-- Insert 2000 rows. Every row contains "alpha beta" so the tokens "alpha" and "beta" each appear
-- in all 2000 docs → 4 large blocks (512 docs each). The phrase "alpha beta" should match all rows.
-- Additionally sprinkle "gamma delta" in rows where id % 7 == 0 to test selective phrase matches.
-- Row text: "alpha beta row_<id>" or "alpha beta gamma delta row_<id>"
INSERT INTO t_phrase_multi_lb
SELECT
    number AS id,
    concat(
        'alpha beta',
        if(number % 7 = 0, ' gamma delta', ''),
        ' row_', toString(number)
    ) AS text
FROM numbers(2000);

SELECT '-- all rows match alpha beta';
SELECT count() FROM t_phrase_multi_lb WHERE hasPhrase(text, 'alpha beta');

SELECT '-- gamma delta rows (every 7th: 0,7,14,...,1995 = 286 rows)';
SELECT count() FROM t_phrase_multi_lb WHERE hasPhrase(text, 'gamma delta');

SELECT '-- three-token phrase crossing large block boundary';
SELECT count() FROM t_phrase_multi_lb WHERE hasPhrase(text, 'beta gamma delta');

SELECT '-- verify specific rows in last large block';
SELECT id FROM t_phrase_multi_lb WHERE hasPhrase(text, 'gamma delta') AND id >= 1990 ORDER BY id;

SELECT '-- verify hasToken still works (sanity)';
SELECT count() FROM t_phrase_multi_lb WHERE hasToken(text, 'alpha');

-- ═══════════════════════════════════════════════════════════════
-- SECTION 2: Merge preserves correctness across large blocks
-- ═══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS t_phrase_multi_lb_merge;
CREATE TABLE t_phrase_multi_lb_merge (id UInt32, text String,
    PROJECTION idx INDEX text TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        enable_phrase_query_support = 1,
        posting_list_block_size = 512)
) ENGINE = MergeTree ORDER BY id;

-- Insert two parts, each large enough to produce multiple large blocks per token
INSERT INTO t_phrase_multi_lb_merge
SELECT number AS id, concat('foo bar baz row_', toString(number)) AS text
FROM numbers(1000);

INSERT INTO t_phrase_multi_lb_merge
SELECT number + 1000 AS id, concat('foo bar baz row_', toString(number + 1000)) AS text
FROM numbers(1000);

OPTIMIZE TABLE t_phrase_multi_lb_merge FINAL;

SELECT '-- after merge: all 2000 rows match foo bar';
SELECT count() FROM t_phrase_multi_lb_merge WHERE hasPhrase(text, 'foo bar');

SELECT '-- after merge: three-token phrase';
SELECT count() FROM t_phrase_multi_lb_merge WHERE hasPhrase(text, 'foo bar baz');

SELECT '-- after merge: specific row from second part';
SELECT id FROM t_phrase_multi_lb_merge WHERE hasPhrase(text, 'bar baz') AND id = 1500;

DROP TABLE t_phrase_multi_lb;
DROP TABLE t_phrase_multi_lb_merge;
