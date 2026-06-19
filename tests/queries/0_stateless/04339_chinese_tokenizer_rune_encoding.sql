-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba, which is not available in the fast test build
--
-- Edge cases for the chinese tokenizer's rune encoding and ASCII handling.

-- Distinct dictionary words must keep distinct entries (an earlier encoding collapsed some
-- pairs, e.g. 一 U+4E00 and 仰 U+4EF0, onto the same key so one lost its multi-char entry).
SELECT '-- distinct dictionary words segment correctly';
SELECT tokens('一头', 'chinese');
SELECT tokens('仰头', 'chinese');
SELECT tokens('一面', 'chinese');
SELECT tokens('仰面', 'chinese');

-- Mixed ASCII alphanumeric runs stay as one token, in both granularities.
SELECT '-- alphanumeric runs stay as one token';
SELECT tokens('5G', 'chinese');
SELECT tokens('5G网络', 'chinese');
SELECT tokens('iPhone6s', 'chinese');
SELECT tokens('H2O很重要', 'chinese');
SELECT tokens('学号123456', 'chinese', 'fine_grained');

-- ASCII punctuation is dropped, not emitted as standalone tokens, in both granularities.
SELECT '-- ASCII punctuation is dropped';
SELECT tokens('hello,world', 'chinese');
SELECT tokens('a;b;c', 'chinese');
SELECT tokens('foo(bar)baz', 'chinese');
SELECT tokens('hello,world', 'chinese', 'fine_grained');

-- Full-width Chinese punctuation is a separator.
SELECT '-- full-width punctuation is a separator';
SELECT tokens('你好，世界', 'chinese');
SELECT tokens('你好。再见', 'chinese');

-- Astral (>BMP) characters must not throw or corrupt token offsets: surrounding tokens are
-- still sliced at the correct byte boundaries (the astral char becomes its own token).
SELECT '-- astral characters do not throw or corrupt offsets';
SELECT tokens('北京😀大学', 'chinese');
SELECT tokens('😀😀', 'chinese');
SELECT tokens('a😀b', 'chinese');
-- Malformed UTF-8 must not throw either. The output is hex-encoded so the reference stays
-- valid UTF-8 while still pinning the exact token bytes (proving offsets are not corrupted).
SELECT '-- malformed bytes do not throw and token bytes stay accurate (hex-encoded)';
SELECT arrayMap(t -> hex(t), tokens(concat('北京', unhex('FF'), '大学'), 'chinese'));
SELECT arrayMap(t -> hex(t), tokens(concat('5G', unhex('C0'), '网络'), 'chinese'));

-- `hasToken` on a chinese-indexed column uses splitByNonAlpha at the row level, so it
-- never matches a Jieba sub-token inside a CJK run; it must agree with and without the index.
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

SELECT count() FROM chinese_idx WHERE hasToken(doc, '网易');
SELECT count() FROM chinese_idx WHERE hasToken(doc, '网易') SETTINGS use_skip_indexes = 0;

-- hasAllTokens / hasAnyTokens tokenize the needle with the index tokenizer, so they find
-- the row and the index is allowed to prune granules.
SELECT id FROM chinese_idx WHERE hasAllTokens(doc, ['网易']) ORDER BY id;
SELECT id FROM chinese_idx WHERE hasAllTokens(doc, ['网易']) ORDER BY id SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE chinese_idx;

-- LIKE / startsWith / endsWith are not supported by the chinese index tokenizer
-- (supportsStringLike() is false); they must fall through to row-level evaluation.
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
