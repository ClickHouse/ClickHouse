-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- `system.dictionaries.query_count` of an XGBoost dictionary counts the rows passed to the model by
-- `predictXGBoost`. A call whose features are all constant is evaluated at most once per block, so it counts
-- at most one row per block, never one per row. `max_block_size` is pinned to make the block count known.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05264_xgb;
DROP TABLE IF EXISTS training_05264;

CREATE TABLE training_05264 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05264 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model_05264_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05264'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

SELECT 'Before any prediction';
SYSTEM RELOAD DICTIONARY model_05264_xgb;
SELECT query_count, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05264_xgb';

SELECT 'No rows to predict';
SELECT predictXGBoost('model_05264_xgb', toFloat64(number), 2.0) FROM numbers(0);
SELECT query_count, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05264_xgb';

SELECT 'One row per predicted row';
SELECT sum(isFinite(predictXGBoost('model_05264_xgb', toFloat64(number), 2.0))) FROM numbers(1000) SETTINGS max_block_size = 65536;
SELECT query_count, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05264_xgb';

SELECT 'All features constant: evaluated at most once per block';
-- 1000 rows in blocks of 100 are 10 blocks. The planner may also fold the call into a single evaluation for the
-- whole query, so the increase is checked to be between one and the number of blocks, rather than an exact count.
SELECT sum(isFinite(predictXGBoost('model_05264_xgb', 1.0, 2.0))) FROM numbers(1000) SETTINGS max_block_size = 100;
SELECT query_count - 1000 BETWEEN 1 AND 10, found_rate FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05264_xgb';

DROP DICTIONARY model_05264_xgb;
DROP TABLE training_05264;
