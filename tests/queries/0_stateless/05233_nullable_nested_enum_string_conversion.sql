SET short_circuit_function_evaluation_for_nulls = 0;

-- Unmatched rows contain invalid enum codes under the `NULL` map.
CREATE TABLE nested_enum_left (k UInt8) ENGINE = Memory;
CREATE TABLE nested_enum_right (k UInt8, t Tuple(Enum8('a' = 1, 'b' = 2)), d Tuple(Tuple(Enum16('x' = 256, 'y' = 257)))) ENGINE = Memory;
INSERT INTO nested_enum_left VALUES (1), (2), (3);
INSERT INTO nested_enum_right VALUES (1, ('a'), (('x'))), (3, ('b'), (('y')));

SELECT l.k, toString(r.t), concat(r.t, '!'), format('{}', r.t), toJSONString(r.t), toString(r.d), toJSONString(r.d)
FROM nested_enum_left AS l LEFT JOIN nested_enum_right AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_use_nulls = 1, join_algorithm = 'hash';

SELECT l.k, toString(r.t), concat(r.t, '!'), format('{}', r.t), toJSONString(r.t), toString(r.d), toJSONString(r.d)
FROM nested_enum_left AS l LEFT JOIN nested_enum_right AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_use_nulls = 1, join_algorithm = 'parallel_hash';

SELECT l.k, toString(r.t), concat(r.t, '!'), format('{}', r.t), toJSONString(r.t), toString(r.d), toJSONString(r.d)
FROM nested_enum_left AS l LEFT JOIN nested_enum_right AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_use_nulls = 1, join_algorithm = 'full_sorting_merge';

SELECT l.k, toString(r.t), concat(r.t, '!'), format('{}', r.t), toJSONString(r.t), toString(r.d), toJSONString(r.d)
FROM nested_enum_left AS l LEFT JOIN nested_enum_right AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_use_nulls = 1, join_algorithm = 'grace_hash';

SELECT l.k, toString(r.t), concat(r.t, '!'), format('{}', r.t), toJSONString(r.t), toString(r.d), toJSONString(r.d)
FROM nested_enum_left AS l LEFT JOIN nested_enum_right AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_use_nulls = 0, join_algorithm = 'hash';

DROP TABLE nested_enum_right;
DROP TABLE nested_enum_left;

-- `INSERT` also produces invalid nested enum codes for `Nullable(Tuple)`.
SET enable_nullable_tuple_type = 1;
CREATE TABLE nullable_nested_enum
(
    k UInt8,
    t Nullable(Tuple(Enum8('a' = 1, 'b' = 2))),
    d Nullable(Tuple(Tuple(Enum16('x' = 256, 'y' = 257))))
) ENGINE = Memory;
INSERT INTO nullable_nested_enum VALUES (1, ('a'), (('x'))), (2, NULL, NULL), (3, ('b'), (('y')));

SELECT k, toString(t), concat(t, '!'), format('{}', t), toJSONString(t), toString(d), toJSONString(d)
FROM nullable_nested_enum ORDER BY k;
SELECT k, toString(t), concat(t, '!'), format('{}', t), toJSONString(t), toString(d), toJSONString(d)
FROM nullable_nested_enum ORDER BY k
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0;

-- Cover constant and all-null columns as well as blocks without nulls.
SELECT toString(CAST(NULL AS Nullable(Tuple(Enum8('a' = 1))))),
       toString(materialize(CAST(NULL AS Nullable(Tuple(Enum8('a' = 1))))));
SELECT toString(t), toJSONString(d) FROM nullable_nested_enum WHERE k != 2 ORDER BY k;

DROP TABLE nullable_nested_enum;
