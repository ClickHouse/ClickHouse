-- More tests for use_skip_index_if_final_exact_mode optimization
-- Test when part does not have final mark, optimization will just expand to all ranges

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;

DROP TABLE IF EXISTS tab1;

-- The CREATE TABLE raises a warning due to index_granularity_bytes = 0
SET send_logs_level = 'fatal';

CREATE TABLE tab1
(
 `valueDate` Date,
 `bb_ticker` String,
 `ric` String,
 `update_timestamp` DateTime,
 INDEX tab1_bb_ticker_idx bb_ticker TYPE bloom_filter GRANULARITY 4,
 INDEX tab1_ric_idx ric TYPE bloom_filter GRANULARITY 4
)
ENGINE = ReplacingMergeTree(update_timestamp)
PRIMARY KEY (valueDate, bb_ticker, ric)
ORDER BY (valueDate, bb_ticker, ric)
SETTINGS index_granularity = 111, index_granularity_bytes = 0, compress_primary_key = 0;

SET send_logs_level = 'warning';

SYSTEM STOP MERGES tab1;

INSERT INTO tab1(valueDate, bb_ticker, ric)
    SELECT today(), number%1111, number%111111
    FROM numbers(1e4);

-- No exception/assert & no rows.
SELECT * FROM tab1 FINAL WHERE ric = 'BOWNU.O';

DROP TABLE tab1;
