SET allow_experimental_inverted_index = 0;
CREATE OR REPLACE TABLE tab
(
    `key` UInt64,
    `str` String
)
ENGINE = MergeTree
ORDER BY key;

ALTER TABLE tab ADD INDEX inv_idx(str) TYPE inverted(0); -- { serverError SUPPORT_IS_DISABLED }
