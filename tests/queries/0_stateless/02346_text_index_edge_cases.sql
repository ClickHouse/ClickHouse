-- Tags: no-fasttest
-- Sanity checks around predicate shapes the text index cannot service. All
-- three predicates below must raise INDEX_NOT_USED when the index is forced
-- via `force_data_skipping_indices`, and the two positive counterparts at
-- the end must still push down:
--   1. `equals(message, '')` / `message == ''` — tokenizing '' yields no
--      tokens, so the text index has no required token to look up. Empty-
--      string equality cannot be served by the index.
--   2. a regex whose required prefix is not a complete token (the `match()`
--      codepath refuses to claim the index when it cannot extract a full
--      required token).
-- Positive counterparts that *do* push down are exercised below as a sanity
-- check.

SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS t_text_idx_extra;

CREATE TABLE t_text_idx_extra
(
    id UInt64,
    message String,
    INDEX idx_message message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_idx_extra VALUES (1, 'v1.0 release notes'), (2, 'beta version');

-- equals('') tokenizes to no tokens; the text index has no required token
-- to look up, so forcing it must raise INDEX_NOT_USED. Both the function-
-- call form and the operator form are covered.
SELECT * FROM t_text_idx_extra WHERE equals(message, '') SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }
SELECT * FROM t_text_idx_extra WHERE message == ''         SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- A regex like `v[0-9]+\.[0-9]+` has no complete required token to push down
-- (the leading `v` alone is shorter than the alphanumeric boundary), so the
-- planner refuses to claim the index helped.
SELECT * FROM t_text_idx_extra WHERE match(message, 'v[0-9]+\.[0-9]+') SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- Sanity check: positive counterparts still use the index.
-- A match() whose pattern contains a complete required token can be pushed down.
SELECT * FROM t_text_idx_extra WHERE match(message, ' release ') SETTINGS force_data_skipping_indices='idx_message';
-- Non-empty equality has a single required token.
SELECT * FROM t_text_idx_extra WHERE equals(message, 'beta version') SETTINGS force_data_skipping_indices='idx_message';

DROP TABLE t_text_idx_extra;
