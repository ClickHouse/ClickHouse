-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- The messages of the XGBoost library go to the server log under the `XGBoost` logger, attached to the query
-- that trains or predicts, instead of to `stderr`. `verbosity 3` makes XGBoost log its debug messages.

SET enable_xgboost = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big

DROP DICTIONARY IF EXISTS model_05263_xgb;
DROP TABLE IF EXISTS training_05263;

CREATE TABLE training_05263 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05263 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model_05263_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05263'))
LAYOUT(XGBOOST(num_iterations 2 verbosity 3))
LIFETIME(0);

-- The dictionary loads, and so trains, on the first `predictXGBoost` call.
SELECT predictXGBoost('model_05263_xgb', 1.0, 2.0) FORMAT Null SETTINGS log_comment = '05263_xgboost_predict';

SYSTEM FLUSH LOGS query_log, text_log;

-- The messages carry the server log's own level and timestamp: XGBoost's level prefix, its timestamp and the
-- trailing newline of its monitor lines are removed.
SELECT
    countIf(level = 'Debug' AND message LIKE '%Using tree method%') > 0,
    countIf(message LIKE '[%' OR message LIKE 'DEBUG:%' OR message LIKE '%\n') = 0
FROM system.text_log
WHERE event_date >= yesterday()
    AND logger_name = 'XGBoost'
    AND query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday()
            AND current_database = currentDatabase()
            AND log_comment = '05263_xgboost_predict'
            AND type = 'QueryFinish');

DROP DICTIONARY model_05263_xgb;
DROP TABLE training_05263;
