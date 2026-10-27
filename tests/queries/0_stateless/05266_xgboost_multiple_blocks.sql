-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.
-- no-parallel-replicas: the dictionary exists only on the initiator, so a query spread over the
-- replicas fails with `Dictionary (model_05266_xgb) not found` on the others.

-- `predictXGBoost` over many small blocks predicts every row exactly as over one block: each block is
-- predicted on its own, so a row's prediction must not depend on the block it lands in.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05266_xgb;
DROP TABLE IF EXISTS training_05266;
DROP TABLE IF EXISTS inference_05266;
DROP TABLE IF EXISTS predictions_05266;

CREATE TABLE training_05266 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05266 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model_05266_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05266'))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

-- Distinct feature vectors, so that a row predicted with another row's features would be noticed.
CREATE TABLE inference_05266 (id UInt64, x1 Float64, x2 Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO inference_05266 SELECT number AS id, number % 120 AS x1, intDiv(number, 97) % 15 AS x2 FROM numbers(10000);

SYSTEM RELOAD DICTIONARY model_05266_xgb;

-- Each side is a separate top-level query, so that its `max_block_size` applies to the read it predicts on.
CREATE TABLE predictions_05266 (pass String, id UInt64, p Float64) ENGINE = MergeTree ORDER BY (pass, id);

INSERT INTO predictions_05266 SELECT 'small', id, predictXGBoost('model_05266_xgb', x1, x2) FROM inference_05266
SETTINGS max_block_size = 7, max_threads = 4;
INSERT INTO predictions_05266 SELECT 'large', id, predictXGBoost('model_05266_xgb', x1, x2) FROM inference_05266
SETTINGS max_block_size = 100000, max_threads = 1;

-- `if` with a NULL branch keeps the NULL to NaN path in the same block-by-block check.
INSERT INTO predictions_05266 SELECT 'small_nullable', id, predictXGBoost('model_05266_xgb', if(id % 5 = 0, NULL, x1), x2) FROM inference_05266
SETTINGS max_block_size = 7, max_threads = 4;
INSERT INTO predictions_05266 SELECT 'large_nullable', id, predictXGBoost('model_05266_xgb', if(id % 5 = 0, NULL, x1), x2) FROM inference_05266
SETTINGS max_block_size = 100000, max_threads = 1;

SELECT 'Small blocks predict every row like one block';
SELECT count(), countIf(small.p = large.p)
FROM (SELECT id, p FROM predictions_05266 WHERE pass = 'small') AS small
INNER JOIN (SELECT id, p FROM predictions_05266 WHERE pass = 'large') AS large USING (id);

SELECT 'Small blocks with Nullable features predict every row like one block';
SELECT count(), countIf(small.p = large.p)
FROM (SELECT id, p FROM predictions_05266 WHERE pass = 'small_nullable') AS small
INNER JOIN (SELECT id, p FROM predictions_05266 WHERE pass = 'large_nullable') AS large USING (id);

SELECT 'Every row of every block is counted';
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_05266_xgb';

DROP DICTIONARY model_05266_xgb;
DROP TABLE training_05266;
DROP TABLE inference_05266;
DROP TABLE predictions_05266;
