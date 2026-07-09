-- accurateCastOrDefault to JSON must honor json_type_escape_dots_in_keys,
-- like accurateCastOrNull / accurateCast already do (issue #109943).

SET json_type_escape_dots_in_keys = 1;

-- Dotted key: with the setting on, the dot must be kept as part of the key
-- (not split into a nested object) on all three cast paths.
SELECT accurateCastOrDefault('{"a.b":1}', 'JSON') AS cast_or_default;
SELECT accurateCastOrNull('{"a.b":1}', 'JSON') AS cast_or_null;
SELECT accurateCast('{"a.b":1}', 'JSON') AS cast;

-- Same via a non-const column.
SELECT accurateCastOrDefault(materialize('{"a.b":1}'), 'JSON');
SELECT accurateCastOrNull(materialize('{"a.b":1}'), 'JSON');

-- Multiple / nested dotted keys.
SELECT accurateCastOrDefault('{"x.y.z":1,"p.q":2}', 'JSON');
SELECT accurateCastOrDefault('{"a.b":{"c.d":3}}', 'JSON');

-- With the setting off the dot splits into a nested object (unchanged behavior).
SET json_type_escape_dots_in_keys = 0;
SELECT accurateCastOrDefault('{"a.b":1}', 'JSON');
SELECT accurateCastOrNull('{"a.b":1}', 'JSON');

-- Boundary: invalid JSON with accurateCastOrDefault yields the default (empty object).
SET json_type_escape_dots_in_keys = 1;
SELECT accurateCastOrDefault('not a json', 'JSON');
SELECT accurateCastOrNull('not a json', 'JSON');
