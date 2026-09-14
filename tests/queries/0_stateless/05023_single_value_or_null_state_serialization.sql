-- Tags: no-replicated-database
-- Tag no-replicated-database: version 0 is not printed in the type name, so the legacy state pin
-- does not survive re-parsing the CREATE query from the replicated database DDL log.

DROP TABLE IF EXISTS single_value_or_null_state_serialization;

CREATE TABLE single_value_or_null_state_serialization
(
    id UInt8,
    state AggregateFunction(singleValueOrNull, UInt64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO single_value_or_null_state_serialization
SELECT 1, singleValueOrNullState(toUInt64(42));

INSERT INTO single_value_or_null_state_serialization
SELECT 0, arrayReduce('singleValueOrNullState', []::Array(UInt64));

INSERT INTO single_value_or_null_state_serialization
SELECT 2, singleValueOrNullState(number)
FROM numbers(2);

SELECT DISTINCT toTypeName(state) FROM single_value_or_null_state_serialization;

SELECT id, singleValueOrNullMerge(state)
FROM single_value_or_null_state_serialization
GROUP BY id
ORDER BY id;

DROP TABLE single_value_or_null_state_serialization;

DROP TABLE IF EXISTS single_value_or_null_legacy_state;

CREATE TABLE single_value_or_null_legacy_state
(
    id UInt8,
    state AggregateFunction(0, singleValueOrNull, UInt64)
)
ENGINE = MergeTree
ORDER BY id;

-- Version 0 cannot distinguish one stored value from a state that saw multiple distinct values.
-- Keep this legacy case to ensure an ambiguous payload never becomes a concrete result.
INSERT INTO single_value_or_null_legacy_state
SELECT 1, arrayReduce('singleValueOrNullState', [toUInt64(42)]);

INSERT INTO single_value_or_null_legacy_state
SELECT 2, arrayReduce('singleValueOrNullState', [toUInt64(42), toUInt64(43)]);

INSERT INTO single_value_or_null_legacy_state
SELECT 3, arrayReduce('singleValueOrNullState', []::Array(UInt64));

SELECT DISTINCT toTypeName(state) FROM single_value_or_null_legacy_state;

SELECT id, singleValueOrNullMerge(state)
FROM single_value_or_null_legacy_state
GROUP BY id
ORDER BY id;

DROP TABLE single_value_or_null_legacy_state;
