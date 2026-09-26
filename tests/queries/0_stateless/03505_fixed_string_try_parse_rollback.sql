-- Regression test: a failed `FixedString` try-parse must leave the target column byte-identical.

-- A failed `Array(Nullable(FixedString))` parse used to leave a phantom nested element with no
-- matching null-map entry, throwing a logical-error exception during `Native` serialization. Returns the `String` variant.
SELECT CAST(materialize('[''a') AS Variant(Array(Nullable(FixedString(1))), String))
SETTINGS cast_string_to_variant_use_inference = 1;

-- A failed parse followed by a successful one into the same block used to yield silently shifted bytes.
SELECT CAST(materialize(arrayJoin(['[''x', '[''ab'']'])) AS Variant(Array(Nullable(FixedString(3))), String)) AS v
ORDER BY toString(v)
SETTINGS cast_string_to_variant_use_inference = 1;
