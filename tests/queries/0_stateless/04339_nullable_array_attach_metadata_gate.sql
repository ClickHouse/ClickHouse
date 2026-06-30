-- Tags: no-random-settings

DROP TABLE IF EXISTS nullable_array_attach_metadata_gate;
DROP TABLE IF EXISTS nullable_array_attach_full_gate;

SET allow_experimental_nullable_array_type = 1;

CREATE TABLE nullable_array_attach_metadata_gate
(
    id UInt8,
    a Nullable(Array(Int32))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO nullable_array_attach_metadata_gate VALUES (1, NULL), (2, [1, 2]);

DETACH TABLE nullable_array_attach_metadata_gate;

SET allow_experimental_nullable_array_type = 0;

ATTACH TABLE nullable_array_attach_metadata_gate;

SELECT throwIf(toTypeName(a) != 'Nullable(Array(Int32))')
FROM nullable_array_attach_metadata_gate
FORMAT Null;

SELECT throwIf(groupArray(isNull(a)) != [1, 0])
FROM (SELECT a FROM nullable_array_attach_metadata_gate ORDER BY id)
FORMAT Null;

ATTACH TABLE nullable_array_attach_full_gate (a Nullable(Array(Int32))) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE nullable_array_attach_metadata_gate;

SELECT 'ok';
