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
    countIf(tupleElement(value, 'value_at_anchor') = 0),
    sum(exponentialTimeDecayingValueAt(value, 0))
FROM time_decay_sparse_serialization;

DROP TABLE time_decay_sparse_serialization;
