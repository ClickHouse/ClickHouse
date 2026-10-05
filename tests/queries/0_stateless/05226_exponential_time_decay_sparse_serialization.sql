SET allow_experimental_time_decay_aggregate_functions = 1;

DROP TABLE IF EXISTS time_decay_sparse_serialization;
CREATE TABLE time_decay_sparse_serialization
(
    id UInt64,
    value ExponentialTimeDecaying64(10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0;

INSERT INTO time_decay_sparse_serialization
SELECT
    number,
    if(
        number = 0,
        exponentialTimeDecaying(10)(1., 0.),
        defaultValueOfTypeName('ExponentialTimeDecaying64(10)'))
FROM numbers(1000);

SELECT serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'time_decay_sparse_serialization'
    AND column = 'value'
    AND active;

SELECT
    count(),
    countIf(tupleElement(value, 'value') = 0),
    sum(exponentialTimeDecayingValueAt(value, 0))
FROM time_decay_sparse_serialization;

DROP TABLE time_decay_sparse_serialization;

-- JSON extraction must preserve the requested physical mode, including when nested.
WITH
    JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying64(3, direct)') AS direct64,
    JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying128(3, direct)') AS direct128,
    JSONExtract('[[8,5.25,3]]', 'Array(ExponentialTimeDecaying64(3, direct))')[1] AS nested64,
    JSONExtract('[[8,5.25,3]]', 'Array(ExponentialTimeDecaying128(3, direct))')[1] AS nested128
SELECT
    tupleElement(direct64, 'value') = 8,
    tupleElement(direct64, 'timestamp') = 5.25,
    tupleElement(direct128, 'value') = 8,
    tupleElement(direct128, 'timestamp') = 5.25,
    tupleElement(nested64, 'value') = 8,
    tupleElement(nested128, 'timestamp') = 5.25;

-- Explicit `compact` uses the same mode parser and retains the default type name.
SELECT
    toTypeName(JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying64(3, compact)')),
    toTypeName(JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying128(3, compact)'));

-- Accepting the parser's bare mode nodes must not accept parameterized modes.
SELECT JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying64(3, direct(1))'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying128(3, compact(1))'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying64(3, invalid)'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtract('[8,5.25,3]', 'ExponentialTimeDecaying128(3, 1)'); -- { serverError BAD_ARGUMENTS }

-- Extracting a nonempty Variant alternative expands the selected column over
-- absent rows. The keys under the resulting null map must remain valid zero
-- curves, because normal nullable evaluation may inspect those nested rows.
SET short_circuit_function_evaluation_for_nulls = 0;

DROP TABLE IF EXISTS time_decay_expanded_defaults;
CREATE TABLE time_decay_expanded_defaults
(
    id UInt8,
    compact64 Variant(ExponentialTimeDecaying64(3), UInt8),
    compact128 Variant(ExponentialTimeDecaying128(3), UInt8),
    direct64 Variant(ExponentialTimeDecaying64(3, direct), UInt8),
    direct128 Variant(ExponentialTimeDecaying128(3, direct), UInt8)
)
ENGINE = Memory;

INSERT INTO time_decay_expanded_defaults VALUES
    (1, (8., 5.25, 3.), (8., 5.25, 3.), (8., 5.25, 3.), (8., 5.25, 3.)),
    (2, 7, 7, 7, 7),
    (3, NULL, NULL, NULL, NULL);

SELECT
    id,
    ifNull(isFinite(exponentialTimeDecayingValueAt(
        variantElement(compact64, 'ExponentialTimeDecaying64(3)'), 0.)), 1),
    ifNull(isFinite(exponentialTimeDecayingValueAt(
        variantElement(compact128, 'ExponentialTimeDecaying128(3)'), 0.)), 1),
    ifNull(isFinite(exponentialTimeDecayingValueAt(
        variantElement(direct64, 'ExponentialTimeDecaying64(3, direct)'), 0.)), 1),
    ifNull(isFinite(exponentialTimeDecayingValueAt(
        variantElement(direct128, 'ExponentialTimeDecaying128(3, direct)'), 0.)), 1)
FROM time_decay_expanded_defaults
ORDER BY id;

-- Unmask the expanded slots to check the logical zero itself. Evaluate every
-- input row before the aggregate condition so the alternative remains nonempty.
SELECT
    sumIf(abs(exponentialTimeDecayingValueAt(assumeNotNull(variantElement(
        compact64, 'ExponentialTimeDecaying64(3)')), 0.)), id != 1) = 0,
    sumIf(abs(exponentialTimeDecayingValueAt(assumeNotNull(variantElement(
        compact128, 'ExponentialTimeDecaying128(3)')), 0.)), id != 1) = 0,
    sumIf(abs(exponentialTimeDecayingValueAt(assumeNotNull(variantElement(
        direct64, 'ExponentialTimeDecaying64(3, direct)')), 0.)), id != 1) = 0,
    sumIf(abs(exponentialTimeDecayingValueAt(assumeNotNull(variantElement(
        direct128, 'ExponentialTimeDecaying128(3, direct)')), 0.)), id != 1) = 0
FROM time_decay_expanded_defaults;

-- Retained Direct rows must keep their original printable metadata after expansion.
SELECT
    sumIf(tupleElement(assumeNotNull(variantElement(
        direct64, 'ExponentialTimeDecaying64(3, direct)')), 'value'), id = 1) = 8,
    sumIf(tupleElement(assumeNotNull(variantElement(
        direct64, 'ExponentialTimeDecaying64(3, direct)')), 'timestamp'), id = 1) = 5.25,
    sumIf(tupleElement(assumeNotNull(variantElement(
        direct128, 'ExponentialTimeDecaying128(3, direct)')), 'value'), id = 1) = 8,
    sumIf(tupleElement(assumeNotNull(variantElement(
        direct128, 'ExponentialTimeDecaying128(3, direct)')), 'timestamp'), id = 1) = 5.25
FROM time_decay_expanded_defaults;

DROP TABLE time_decay_expanded_defaults;
