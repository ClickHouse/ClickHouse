-- Tags: no-fasttest
-- Verify that text index does not support negated functions (notEquals, notLike, NOT IN)
-- after removing their support in d12905698ae.

DROP TABLE IF EXISTS t_text_idx_neg;

CREATE TABLE t_text_idx_neg
(
    id UInt64,
    message String,
    INDEX idx_message message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_idx_neg VALUES (1, 'Service is not ready');

-- notEquals: text index cannot be used
SELECT * FROM t_text_idx_neg WHERE notEquals(message, 'foo') SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- notLike: text index cannot be used
SELECT * FROM t_text_idx_neg WHERE message NOT LIKE '%foo%' SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- NOT IN: text index cannot be used
SELECT * FROM t_text_idx_neg WHERE message NOT IN ('foo', 'bar') SETTINGS force_data_skipping_indices='idx_message'; -- { serverError INDEX_NOT_USED }

-- Sanity check: positive counterparts still use the index
SELECT * FROM t_text_idx_neg WHERE equals(message, 'nonexistent string') SETTINGS force_data_skipping_indices='idx_message';
SELECT * FROM t_text_idx_neg WHERE message LIKE '%Service%is%' SETTINGS force_data_skipping_indices='idx_message';
SELECT * FROM t_text_idx_neg WHERE message IN ('Service is not ready') SETTINGS force_data_skipping_indices='idx_message';

DROP TABLE t_text_idx_neg;
