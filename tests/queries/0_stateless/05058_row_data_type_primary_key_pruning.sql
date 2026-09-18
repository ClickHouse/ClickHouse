SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_key_prune;

CREATE TABLE row_key_prune
(
    r Row(x UInt64, y String)
)
ENGINE = MergeTree ORDER BY r
SETTINGS index_granularity = 1;

INSERT INTO row_key_prune SELECT (number, concat('v', toString(number))) FROM numbers(16);

-- The primary key must be used (force_primary_key) and must actually prune granules:
-- with index_granularity = 1, max_rows_to_read caps how many granules a query may touch.

SELECT r FROM row_key_prune WHERE r = (3, 'v3') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT r FROM row_key_prune WHERE r = (3, 'v3') SETTINGS force_primary_key = 1, max_rows_to_read = 2, enable_analyzer = 0;

SELECT r FROM row_key_prune WHERE r < (2, '') ORDER BY r SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT r FROM row_key_prune WHERE r >= (14, 'v14') ORDER BY r SETTINGS force_primary_key = 1, max_rows_to_read = 3;

SELECT r FROM row_key_prune WHERE r IN ((3, 'v3'), (5, 'v5')) ORDER BY r SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT r FROM row_key_prune WHERE r IN ((3, 'v3'), (5, 'v5')) ORDER BY r SETTINGS force_primary_key = 1, max_rows_to_read = 4, enable_analyzer = 0;

DROP TABLE row_key_prune;

-- A named Tuple key takes the same analysis path a lowered Row does. Equality and range
-- predicates on it used to decline index analysis, because the least supertype with an
-- unnamed constant tuple drops the element names and the name-sensitive type comparison
-- then demanded a cast without monotonicity information.

DROP TABLE IF EXISTS named_tuple_key_prune;

CREATE TABLE named_tuple_key_prune
(
    t Tuple(a UInt64, b String)
)
ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1;

INSERT INTO named_tuple_key_prune SELECT (number, concat('v', toString(number))) FROM numbers(16);

SELECT t FROM named_tuple_key_prune WHERE t = (3, 'v3') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT t FROM named_tuple_key_prune WHERE t < (2, '') ORDER BY t SETTINGS force_primary_key = 1, max_rows_to_read = 3;

DROP TABLE named_tuple_key_prune;
