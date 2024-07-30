-- Tests that the inverted index can only be supported when allow_experimental_full_text_index = 1.

SET allow_experimental_full_text_index = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    `key` UInt64,
    `str` String
)
ENGINE = MergeTree
ORDER BY key;

ALTER TABLE tab ADD INDEX inv_idx(str) TYPE full_text(0); -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE tab;
