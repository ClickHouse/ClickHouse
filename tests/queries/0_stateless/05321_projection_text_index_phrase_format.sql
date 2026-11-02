-- Projection text indexes: `hasPhrase` is evaluated exactly only over the phrase-capable format,
-- and an explicit posting list codec that a projection part cannot be loaded with is rejected.

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

-- The projection does not support the `pfor` codec.
CREATE TABLE t_pfor (id UInt32, s String, PROJECTION p INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'pfor'))
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- With `enable_phrase_query_support = 1` the phrase is matched in order, with repeated tokens.
DROP TABLE IF EXISTS t_phrase;
CREATE TABLE t_phrase (id UInt32, s String, PROJECTION p INDEX s TYPE text(tokenizer = 'splitByNonAlpha', enable_phrase_query_support = 1))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_phrase VALUES (1, 'quick brown fox'), (2, 'brown quick fox'), (3, 'quick and brown'), (4, 'the the cat'), (5, 'the cat the');
SELECT id FROM t_phrase WHERE hasPhrase(s, 'quick brown') ORDER BY id;
SELECT id FROM t_phrase WHERE hasPhrase(s, 'brown quick') ORDER BY id;
SELECT id FROM t_phrase WHERE hasPhrase(s, 'the the cat') ORDER BY id;
DROP TABLE t_phrase;
