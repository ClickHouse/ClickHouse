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

DROP TABLE IF EXISTS single_value_or_null_legacy_unversioned;

CREATE TABLE single_value_or_null_legacy_unversioned
(
    id UInt8,
    state AggregateFunction(singleValueOrNull, UInt64)
)
ENGINE = MergeTree
ORDER BY id;

-- Seed a valid current-version part before simulating old metadata so mutation coverage has a row.
INSERT INTO single_value_or_null_legacy_unversioned
SELECT 0, singleValueOrNullState(toUInt64(42));

-- Restating the type without a version simulates metadata written before singleValueOrNull state versioning.
ALTER TABLE single_value_or_null_legacy_unversioned
    MODIFY COLUMN state AggregateFunction(singleValueOrNull, UInt64);

DETACH TABLE single_value_or_null_legacy_unversioned;
ATTACH TABLE single_value_or_null_legacy_unversioned;

SELECT type
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'single_value_or_null_legacy_unversioned'
    AND name = 'state';

INSERT INTO single_value_or_null_legacy_unversioned
SELECT 1, singleValueOrNullState(toUInt64(42)); -- { serverError ILLEGAL_COLUMN }

-- Mutations bypass InsertDependenciesBuilder. They must reject a regenerated legacy state too.
ALTER TABLE single_value_or_null_legacy_unversioned
    UPDATE state = arrayReduce('singleValueOrNullState', [toUInt64(43)])
    WHERE id = 0; -- { serverError ILLEGAL_COLUMN }

DROP VIEW IF EXISTS single_value_or_null_legacy_mv;
DROP TABLE IF EXISTS single_value_or_null_legacy_mv_source;

CREATE TABLE single_value_or_null_legacy_mv_source
(
    value UInt64
)
ENGINE = Memory;

CREATE MATERIALIZED VIEW single_value_or_null_legacy_mv
TO single_value_or_null_legacy_unversioned
AS SELECT
    toUInt8(1) AS id,
    singleValueOrNullState(value) AS state
FROM single_value_or_null_legacy_mv_source
GROUP BY id;

INSERT INTO single_value_or_null_legacy_mv_source VALUES (42); -- { serverError ILLEGAL_COLUMN }

DROP VIEW single_value_or_null_legacy_mv;
DROP TABLE single_value_or_null_legacy_mv_source;

ALTER TABLE single_value_or_null_legacy_unversioned
    MODIFY COLUMN state AggregateFunction(1, singleValueOrNull, UInt64);

INSERT INTO single_value_or_null_legacy_unversioned
SELECT 1, singleValueOrNullState(toUInt64(42));

SELECT type
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'single_value_or_null_legacy_unversioned'
    AND name = 'state';

SELECT singleValueOrNullMerge(state)
FROM single_value_or_null_legacy_unversioned;

DROP TABLE single_value_or_null_legacy_unversioned;

DROP TABLE IF EXISTS single_value_or_null_tuple_legacy_unversioned;

CREATE TABLE single_value_or_null_tuple_legacy_unversioned
(
    id UInt8,
    state AggregateFunction(singleValueOrNullTuple, Tuple(UInt64, UInt64))
)
ENGINE = MergeTree
ORDER BY id;

-- Restating the tuple type without a version simulates metadata written before singleValueOrNull state versioning.
ALTER TABLE single_value_or_null_tuple_legacy_unversioned
    MODIFY COLUMN state AggregateFunction(singleValueOrNullTuple, Tuple(UInt64, UInt64));

DETACH TABLE single_value_or_null_tuple_legacy_unversioned;
ATTACH TABLE single_value_or_null_tuple_legacy_unversioned;

SELECT type
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'single_value_or_null_tuple_legacy_unversioned'
    AND name = 'state';

INSERT INTO single_value_or_null_tuple_legacy_unversioned
SELECT 1, singleValueOrNullTupleState(tuple(toUInt64(42), toUInt64(43))); -- { serverError ILLEGAL_COLUMN }

ALTER TABLE single_value_or_null_tuple_legacy_unversioned
    MODIFY COLUMN state AggregateFunction(1, singleValueOrNullTuple, Tuple(UInt64, UInt64));

INSERT INTO single_value_or_null_tuple_legacy_unversioned
SELECT 1, singleValueOrNullTupleState(tuple(toUInt64(42), toUInt64(43)));

SELECT type
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'single_value_or_null_tuple_legacy_unversioned'
    AND name = 'state';

SELECT singleValueOrNullTupleMerge(state)
FROM single_value_or_null_tuple_legacy_unversioned;

DROP TABLE single_value_or_null_tuple_legacy_unversioned;


DROP TABLE IF EXISTS single_value_or_null_legacy_memory;

CREATE TABLE single_value_or_null_legacy_memory
(
    id UInt8,
    state AggregateFunction(singleValueOrNull, UInt64)
)
ENGINE = Memory;

-- Memory keeps inserted blocks in RAM, but BACKUP later serializes them through NativeWriter.
-- Reject fresh writes while the table metadata still pins the unsafe legacy state format.
ALTER TABLE single_value_or_null_legacy_memory
    MODIFY COLUMN state AggregateFunction(singleValueOrNull, UInt64);

DETACH TABLE single_value_or_null_legacy_memory;
ATTACH TABLE single_value_or_null_legacy_memory;

SELECT type
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'single_value_or_null_legacy_memory'
    AND name = 'state';

INSERT INTO single_value_or_null_legacy_memory
SELECT 1, singleValueOrNullState(toUInt64(42)); -- { serverError ILLEGAL_COLUMN }

ALTER TABLE single_value_or_null_legacy_memory
    MODIFY COLUMN state AggregateFunction(1, singleValueOrNull, UInt64);

INSERT INTO single_value_or_null_legacy_memory
SELECT 1, singleValueOrNullState(toUInt64(42));

SELECT singleValueOrNullMerge(state)
FROM single_value_or_null_legacy_memory;

DROP TABLE single_value_or_null_legacy_memory;
