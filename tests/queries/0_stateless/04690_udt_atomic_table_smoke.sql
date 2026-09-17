-- Tags: memory-engine

SET allow_experimental_user_defined_types = 1;

DROP DATABASE IF EXISTS udt_04690_atomic_table SYNC;
CREATE DATABASE udt_04690_atomic_table ENGINE = Atomic;
CREATE TYPE udt_04690_atomic_table.UserId AS UInt64;
CREATE TYPE udt_04690_atomic_table.Box(T TYPE) AS Tuple(value T);

SET allow_experimental_user_defined_types = 0;
CREATE TABLE udt_04690_atomic_table.disabled_probe
(
    id udt_04690_atomic_table.UserId
)
ENGINE = Memory; -- { serverError SUPPORT_IS_DISABLED }
EXISTS TABLE udt_04690_atomic_table.disabled_probe;

SET allow_experimental_user_defined_types = 1;
CREATE TABLE udt_04690_atomic_table.events
(
    id udt_04690_atomic_table.UserId,
    ids Array(udt_04690_atomic_table.UserId),
    pair Tuple(owner udt_04690_atomic_table.UserId, optional Nullable(udt_04690_atomic_table.UserId)),
    boxed udt_04690_atomic_table.Box(Nullable(udt_04690_atomic_table.UserId))
)
ENGINE = Memory;

INSERT INTO udt_04690_atomic_table.events VALUES
    (1, [1, 10], tuple(1, 11), tuple(100)),
    (2, [2, 20], tuple(2, NULL), tuple(NULL));

SELECT id, arraySum(ids), pair.owner, pair.optional, boxed.value
FROM udt_04690_atomic_table.events
ORDER BY id;

SELECT name, type, if(udt_declared_type = '', '<empty>', udt_declared_type) AS udt_declared_type
FROM system.columns
WHERE database = 'udt_04690_atomic_table' AND table = 'events'
ORDER BY position
FORMAT TSVRaw;

ALTER TABLE udt_04690_atomic_table.events
    ADD COLUMN extra udt_04690_atomic_table.UserId DEFAULT id;
ALTER TABLE udt_04690_atomic_table.events
    MODIFY COLUMN extra Nullable(udt_04690_atomic_table.UserId);
SELECT default_kind, default_expression
FROM system.columns
WHERE database = 'udt_04690_atomic_table' AND table = 'events' AND name = 'extra'
FORMAT TSVRaw;
SELECT sum(id), sum(extra), countIf(isNull(extra))
FROM udt_04690_atomic_table.events;

ALTER TYPE udt_04690_atomic_table.UserId RENAME TO PrincipalId;
RENAME TABLE udt_04690_atomic_table.events TO udt_04690_atomic_table.renamed_events;

SELECT table, name, type, if(udt_declared_type = '', '<empty>', udt_declared_type) AS udt_declared_type
FROM system.columns
WHERE database = 'udt_04690_atomic_table' AND table = 'renamed_events'
ORDER BY position
FORMAT TSVRaw;

SELECT toTypeName(id), toTypeName(extra)
FROM udt_04690_atomic_table.renamed_events
LIMIT 1;
SELECT sum(id), sum(extra)
FROM udt_04690_atomic_table.renamed_events;

DROP TABLE udt_04690_atomic_table.renamed_events SYNC;
DROP TYPE udt_04690_atomic_table.Box RESTRICT;
DROP TYPE udt_04690_atomic_table.PrincipalId RESTRICT;
DROP DATABASE udt_04690_atomic_table SYNC;
