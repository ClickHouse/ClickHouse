-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba, which is not available in the fast test build
--
-- Regression tests for the rewritten chinese tokenizer rune encoding and ASCII handling.
-- These exercise edge cases that were broken by the previous (non-injective) `0x00 -> 0xF0`
-- byte-replacement encoding and the digit-vs-letter split in `matchAlphaOrDigitSeq`:
--
--  1. `一` (U+4E00) and `仰` (U+4EF0) used to collapse to the same trie key, so one of them
--     would lose its dictionary entry and segment as a single character. With the injective
--     3-byte encoding both keep their multi-character entries.
--  2. The HMM emission table used to be indexed by the original codepoint while the runtime
--     looked up the remapped one, so characters with a `0x00` low byte (e.g. `一` U+4E00)
--     read a "missing" probability slot and segmented oddly. After dropping the remap, the
--     emissions are looked up at the correct codepoint.
--  3. Mixed alphanumeric ASCII runs such as `5G`, `iPhone6s`, `H2O` used to be split between
--     the digit and letter portions; they should now stay as a single token.
--  4. ASCII punctuation should be dropped, not emitted as standalone tokens.

-- Words whose pre-fix encoding collided (the 0x00 -> 0xF0 remap turned U+4E00 / U+4EF0
-- into the same trie key, so dictionary entries like `一头` and `仰头` were silently
-- merged and the loser segmented as single characters). With injective encoding both
-- multi-character entries survive and segment correctly.
SELECT '-- previously-colliding dictionary words segment correctly';
SELECT tokens('一头', 'chinese');
SELECT tokens('仰头', 'chinese');
SELECT tokens('一面', 'chinese');
SELECT tokens('仰面', 'chinese');

-- Mixed alphanumeric runs stay as a single token (previously digit-then-letter was split).
SELECT '-- alphanumeric runs stay as one token';
SELECT tokens('5G', 'chinese');
SELECT tokens('5G网络', 'chinese');
SELECT tokens('iPhone6s', 'chinese');
SELECT tokens('H2O很重要', 'chinese');

-- ASCII punctuation is dropped, not emitted as a standalone token.
SELECT '-- ASCII punctuation is dropped';
SELECT tokens('hello,world', 'chinese');
SELECT tokens('a;b;c', 'chinese');
SELECT tokens('foo(bar)baz', 'chinese');

-- Full-width Chinese punctuation is treated as a separator (already a separator before,
-- but exercised here so we notice if the separators set regresses).
SELECT '-- full-width punctuation is a separator';
SELECT tokens('你好，世界', 'chinese');
SELECT tokens('你好。再见', 'chinese');

-- `hasToken` on a chinese-indexed column is documented to use splitByNonAlpha at the row
-- level, so it never matches a Jieba sub-token inside a CJK run. The text index used to
-- "answer" via Exact direct read using Jieba tokens and return wrong results; after the
-- fix the index is bypassed for `hasToken` so the row-level evaluation is consistent
-- regardless of whether the index exists.
SELECT '-- hasToken on chinese text index agrees with row-level evaluation';
DROP TABLE IF EXISTS chinese_idx;
CREATE TABLE chinese_idx
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = 'chinese') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO chinese_idx VALUES (1, '他来到了网易杭研大厦'), (2, '我来自北京邮电大学');

-- hasToken: the needle is not a whole splitByNonAlpha-token of the doc, so the answer
-- must be 0 with and without the index. Before the fix, the index would say "yes" via
-- exact direct read because Jieba tokenizes `网易` as a token of the doc.
SELECT count() FROM chinese_idx WHERE hasToken(doc, '网易');
SELECT count() FROM chinese_idx WHERE hasToken(doc, '网易') SETTINGS use_skip_indexes = 0;

-- hasAllTokens / hasAnyTokens DO tokenize the needle with the index tokenizer, so they
-- find the row and the index is allowed to prune granules.
SELECT id FROM chinese_idx WHERE hasAllTokens(doc, ['网易']) ORDER BY id;
SELECT id FROM chinese_idx WHERE hasAllTokens(doc, ['网易']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE chinese_idx;

-- substring/prefix/suffix paths must not be reachable for the chinese tokenizer — the
-- index condition code is gated on `supportsStringLike()`, which returns false here.
-- Verify that user-facing functions that would route through `substringToTokens`
-- silently bypass the index (return the row from row-level evaluation) instead of
-- crashing or returning wrong answers.
SELECT '-- startsWith / endsWith / like fall through to row-level evaluation';
DROP TABLE IF EXISTS chinese_idx2;
CREATE TABLE chinese_idx2
(
    id UInt32,
    doc String,
    INDEX idx doc TYPE text(tokenizer = 'chinese') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO chinese_idx2 VALUES (1, '北京邮电大学'), (2, '清华大学');
SELECT id FROM chinese_idx2 WHERE startsWith(doc, '北京') ORDER BY id;
SELECT id FROM chinese_idx2 WHERE endsWith(doc, '大学') ORDER BY id;
SELECT id FROM chinese_idx2 WHERE doc LIKE '%邮电%' ORDER BY id;
DROP TABLE chinese_idx2;
