-- Tags: no-fasttest
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

-- An XGBOOST dictionary survives a BACKUP / RESTORE round trip, and the restore is not blocked by
-- `enable_xgboost`.
--
-- The experimental setting gates introducing a new definition (`CREATE DICTIONARY`) and every
-- prediction, but it must not gate restoring metadata that already exists: a backup taken while the
-- feature was enabled has to be restorable on a server where the setting is off, otherwise the
-- setting turns into a trap that makes a backup unrestorable.
--
-- The model itself is not stored in the backup - the dictionary retrains from the restored source
-- table when it is next loaded - so the assertions check that the restored dictionary predicts its
-- target rather than comparing to a stored model.

SET enable_xgboost = 1;

DROP DICTIONARY IF EXISTS model_05056;
DROP TABLE IF EXISTS training_05056;

CREATE TABLE training_05056
(
    x1 Float64,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO training_05056 (x1, x2, y)
SELECT
    number % 10 AS x1,
    intDiv(number, 10) AS x2,
    x1 + 10 * x2 AS y
FROM numbers(100);

CREATE DICTIONARY model_05056 (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_05056'))
LAYOUT(XGBOOST(num_iterations 100 max_depth 6 seed 42))
LIFETIME(0);

SELECT 'The dictionary predicts before the backup';
SELECT abs(predictXGBoost('model_05056', 3.0, 4.0) - 43) < 1;

BACKUP TABLE training_05056, DICTIONARY model_05056 TO Memory('backup_05056') FORMAT Null;

DROP DICTIONARY model_05056;
DROP TABLE training_05056;

-- With the setting off, a new CREATE would be rejected, but restoring existing metadata must not be.
SET enable_xgboost = 0;

RESTORE TABLE training_05056, DICTIONARY model_05056 FROM Memory('backup_05056') FORMAT Null;

SELECT 'RESTORE succeeds while enable_xgboost is off';

-- The setting still governs every prediction, so the restored dictionary cannot be used until it is
-- enabled again.
SELECT predictXGBoost('model_05056', 3.0, 4.0); -- { serverError SUPPORT_IS_DISABLED }

SET enable_xgboost = 1;

SELECT 'The restored dictionary predicts the same target';
SELECT abs(predictXGBoost('model_05056', 3.0, 4.0) - 43) < 1;
SELECT abs(predictXGBoost('model_05056', 4.0, 3.0) - 34) < 1;

DROP DICTIONARY model_05056;
DROP TABLE training_05056;
