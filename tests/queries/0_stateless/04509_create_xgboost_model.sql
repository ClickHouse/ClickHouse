-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the XGBoost contrib, which is not built in the fast test.
-- no-parallel-replicas: the dictionary exists only on the initiator, so a query spread
-- over the replicas fails with `Dictionary (model_04509_xgb) not found` on the others.

-- The XGBoost integration is experimental and must be enabled explicitly.
SET enable_xgboost = 1;

-- Note on why the prediction is never wrapped in count(): count() over a non-nullable column is optimized
-- to count the rows without evaluating its argument, so count(predictXGBoost(...)) would skip the
-- prediction entirely - and with it the dictionary load, feature-count check, and parameter validation
-- that happen when the function executes. Every query below therefore consumes the predicted value
-- (sum(isFinite(...)), a scalar SELECT, or an equality) so the prediction actually runs.

DROP DICTIONARY IF EXISTS model_04509_xgb;
DROP DICTIONARY IF EXISTS model_04509_bad;
DROP DICTIONARY IF EXISTS model_04509_not_xgb;
DROP DICTIONARY IF EXISTS model_04509_eager;
DROP DICTIONARY IF EXISTS model_04509_f32;

DROP TABLE IF EXISTS training_04509;
DROP TABLE IF EXISTS inference_04509;
DROP TABLE IF EXISTS training_04509_non_numeric;
DROP TABLE IF EXISTS not_xgb_04509_src;

CREATE TABLE training_04509
(
    x1 Float64,
    x2 Float64,
    y Float64
)
ENGINE = MergeTree
ORDER BY tuple();

-- Small deterministic dataset: y = 2 * x1 + 3 * x2.
INSERT INTO training_04509 (x1, x2, y)
SELECT
    number AS x1,
    number * 2 AS x2,
    2 * x1 + 3 * x2 AS y
FROM numbers(100);

-- Feature-only table for inference: exactly the feature columns, no target.
CREATE TABLE inference_04509
(
    x1 Float64,
    x2 Float64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO inference_04509 (x1, x2)
SELECT number AS x1, number * 2 AS x2 FROM numbers(10);

SELECT 'Positive: an XGBOOST dictionary with explicit hyperparameters';

CREATE DICTIONARY model_04509_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(max_depth 4 eta 0.3 objective 'reg:squarederror' num_iterations 10))
LIFETIME(0);

-- `predictXGBoost` is a row-wise function returning one Float64 per input row. Exact XGBoost outputs
-- are platform-dependent, instead assert that: every row predicts a finite
-- value, and the result type is Float64.
SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2))) FROM inference_04509;
SELECT any(toTypeName(predictXGBoost('model_04509_xgb', x1, x2))) FROM inference_04509;

-- The dictionary holds a model, not rows, so the generic dictionary interface is unsupported: predictXGBoost
-- is the only way to query it.
SELECT 'Error: the generic dictionary interface is not supported';
SELECT dictGet('model_04509_xgb', 'y', (1.0, 2.0)); -- { serverError UNSUPPORTED_METHOD }
SELECT dictGetFloat64('model_04509_xgb', 'y', (1.0, 2.0)); -- { serverError UNSUPPORTED_METHOD }
SELECT dictGetOrDefault('model_04509_xgb', 'y', (1.0, 2.0), 0.0); -- { serverError UNSUPPORTED_METHOD }
SELECT dictHas('model_04509_xgb', (1.0, 2.0)); -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM model_04509_xgb; -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM dictionary('model_04509_xgb'); -- { serverError UNSUPPORTED_METHOD }

SELECT 'Positive: default hyperparameters (empty layout)';

DROP DICTIONARY model_04509_xgb;
CREATE DICTIONARY model_04509_xgb (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST())
LIFETIME(0);

SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2))) FROM inference_04509;

SELECT 'Positive: predict with prediction parameters';

SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2, map('iteration_begin', 0, 'iteration_end', 0)))) FROM inference_04509;
SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2, map('type', 0)))) FROM inference_04509;

-- Every prediction parameter is an integer or a boolean, so a Bool and a narrower Int type are accepted too.
SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2, map('type', false)))) FROM inference_04509;
SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2, map('iteration_end', toInt8(1))))) FROM inference_04509;

-- `iteration_begin` and `iteration_end` address the boosting rounds of the model, so every round of the
-- default 100 is a valid bound.
SELECT sum(isFinite(predictXGBoost('model_04509_xgb', x1, x2, map('iteration_begin', 1, 'iteration_end', 100)))) FROM inference_04509;

SELECT 'Negative: prediction parameters';

SELECT 'Error: unknown or forbidden prediction parameter';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('not_a_predict_param', 1)); -- { serverError BAD_ARGUMENTS }

SELECT 'Error: prediction parameter Map value is not numeric';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('type', 'x')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Every prediction parameter is an integer or a boolean, so the trailing params Map must have an integer
-- value type.
SELECT 'Error: prediction parameter Map value is not an integer';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', 2.9)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('type', 1.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_begin', 0.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A single fractional value promotes the whole Map to Float64, so the Map as a whole is rejected.
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('type', 1, 'iteration_end', 2.5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Decimal and wide integer values are not accepted either.
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', toDecimal32(1, 2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', toInt128(1))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Error: an unsigned prediction parameter value that does not fit in Int64';
-- Rejected instead of wrapping around to a negative one.
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', toUInt64(18446744073709551615))); -- { serverError BAD_ARGUMENTS }

SELECT 'Error: an iteration bound outside the boosting rounds of the model';
-- XGBoost narrows both bounds to Int32 and range-checks only `iteration_end`, so an out-of-range
-- `iteration_begin` would otherwise reach an unchecked index into the per-round tree offsets.
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_begin', 101)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_begin', 2147483648, 'iteration_end', 0)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_begin', -1)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', 101)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('iteration_end', 2147483648)); -- { serverError BAD_ARGUMENTS }

SELECT 'Error: prediction type other than 0 (value) or 1 (margin) emits several values per row and is unsupported';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('type', 2)); -- { serverError BAD_ARGUMENTS }
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map('type', 4)); -- { serverError BAD_ARGUMENTS }

SELECT 'Error: prediction parameter Map key is not a String';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, map(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Negative: predictXGBoost arguments';

SELECT 'Error: a feature argument is not numeric';
SELECT predictXGBoost('model_04509_xgb', 'x', 2.0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Error: predicting against a dictionary that does not exist';
SELECT predictXGBoost('model_04509_missing', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }

SELECT 'Error: feature count mismatch (more features supplied than the model expects)';
SELECT predictXGBoost('model_04509_xgb', 1.0, 2.0, 3.0); -- { serverError BAD_ARGUMENTS }

SELECT 'Negative: predictXGBoost against a non-XGBoost dictionary';

CREATE TABLE not_xgb_04509_src (id UInt64, val Float64) ENGINE = Memory;
INSERT INTO not_xgb_04509_src VALUES (1, 42);

CREATE DICTIONARY model_04509_not_xgb (id UInt64, val Float64 DEFAULT 0)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'not_xgb_04509_src')) LAYOUT(FLAT()) LIFETIME(0);

-- The dictionary loads fine for its own layout.
SELECT dictGet('model_04509_not_xgb', 'val', toUInt64(1));

-- But predictXGBoost rejects it because it is not an XGBOOST dictionary.
SELECT predictXGBoost('model_04509_not_xgb', 1.0); -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY model_04509_not_xgb;
DROP TABLE not_xgb_04509_src;

SELECT 'Negative: bad hyperparameters (rejected when the model trains, at first use)';

SELECT 'Error: unknown or forbidden training parameter';
CREATE DICTIONARY model_04509_bad (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(not_a_training_param 1)) LIFETIME(0);
SELECT predictXGBoost('model_04509_bad', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_04509_bad;

SELECT 'Error: num_iterations must be a positive integer';
CREATE DICTIONARY model_04509_bad (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(num_iterations 0)) LIFETIME(0);
SELECT predictXGBoost('model_04509_bad', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_04509_bad;

-- A multiclass objective needs 'num_class', which is not an accepted training parameter, because a
-- dictionary predicts exactly one Float64 per row.
SELECT 'Error: a multiclass objective is rejected, because num_class is not an accepted training parameter';
CREATE DICTIONARY model_04509_bad (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(objective 'multi:softmax')) LIFETIME(0);
SELECT predictXGBoost('model_04509_bad', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_04509_bad;

SELECT 'Negative: an integer target attribute (rejected at first use)';

-- A prediction is a floating-point value, so an integer target column would misdescribe what the model
-- predicts.
SELECT 'Error: only Float32 and Float64 are accepted as the target';
CREATE DICTIONARY model_04509_bad (x1 Float64, x2 Float64, y UInt8 DEFAULT 0)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST()) LIFETIME(0);
SELECT predictXGBoost('model_04509_bad', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_04509_bad;

SELECT 'Positive: a Float32 target is accepted';

-- XGBoost computes in single precision, so a Float32 target describes the model exactly; the prediction is a
-- Float64 either way.
CREATE DICTIONARY model_04509_f32 (x1 Float64, x2 Float64, y Float32)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(objective 'reg:squarederror' num_iterations 10)) LIFETIME(0);
SELECT sum(isFinite(predictXGBoost('model_04509_f32', x1, x2))) FROM inference_04509;
SELECT any(toTypeName(predictXGBoost('model_04509_f32', x1, x2))) FROM inference_04509;
DROP DICTIONARY model_04509_f32;

SELECT 'Negative: non-numeric structure (rejected at first use)';

CREATE TABLE training_04509_non_numeric
(
    x1 String,
    x2 String,
    y String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO training_04509_non_numeric VALUES ('a0', 'b0', 'c0'), ('a1', 'b1', 'c1');

CREATE DICTIONARY model_04509_bad (x1 String, x2 String, y String DEFAULT '')
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509_non_numeric'))
LAYOUT(XGBOOST()) LIFETIME(0);
-- Numeric literal features so the function's own argument-type check passes; the dictionary load then
-- rejects the String key/attribute structure with BAD_ARGUMENTS.
SELECT predictXGBoost('model_04509_bad', 1.0, 2.0); -- { serverError BAD_ARGUMENTS }
DROP DICTIONARY model_04509_bad;

SELECT 'Train on demand with SYSTEM RELOAD DICTIONARY';

-- By default the model is trained lazily, on first use. SYSTEM RELOAD DICTIONARY trains it synchronously
-- on demand instead: after the reload the dictionary is LOADED before any predictXGBoost call uses it, and
-- a bad configuration is rejected at the reload (which rethrows the training/config error) rather than on
-- first use.

CREATE DICTIONARY model_04509_eager (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(objective 'reg:squarederror' num_iterations 10))
LIFETIME(0);

-- Trained by the reload: the dictionary is LOADED before any predictXGBoost call uses it.
SYSTEM RELOAD DICTIONARY model_04509_eager;
SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'model_04509_eager';
SELECT isFinite(predictXGBoost('model_04509_eager', 1.0, 2.0));

-- A computational dictionary stores no items, so there is nothing to count or to size.
SELECT 'Positive: system.dictionaries reports no stored items';
SELECT element_count, bytes_allocated FROM system.dictionaries
WHERE database = currentDatabase() AND name = 'model_04509_eager';
DROP DICTIONARY model_04509_eager;

-- A bad configuration is rejected at SYSTEM RELOAD DICTIONARY, which forces training synchronously and
-- rethrows the error.

SELECT 'Error: num_iterations must be a positive integer, rejected at SYSTEM RELOAD DICTIONARY';
CREATE DICTIONARY model_04509_eager (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_04509'))
LAYOUT(XGBOOST(num_iterations 0)) LIFETIME(0);
SYSTEM RELOAD DICTIONARY model_04509_eager; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY model_04509_eager;
DROP DICTIONARY model_04509_xgb;
DROP TABLE training_04509_non_numeric;
DROP TABLE training_04509;
DROP TABLE inference_04509;
