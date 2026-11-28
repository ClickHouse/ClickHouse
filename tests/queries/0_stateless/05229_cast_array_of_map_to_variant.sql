-- A `Map(K, V)` is physically an `Array(Tuple(K, V))` and `CAST` converts between the two spellings,
-- so a `Map` in one of the types may or may not be spelled out as an array by the other one.
-- Counting a `Map` as one dimension unconditionally made the nesting check of `CAST AS Array` reject
-- an `Array(Map(...))` whose target element type keeps the `Map` behind a `Variant`, a `Dynamic`,
-- a `JSON` or a `String`.

SELECT CAST([map('a', 'x')], 'Array(Variant(String, Map(String, String)))');
SELECT CAST([map('k', ['v'])], 'Array(Variant(String, Map(String, Array(String))))');
SELECT CAST(materialize([map('a', 'x')]), 'Array(Variant(String, Map(String, String)))');
SELECT CAST([[map('a', 'x')]], 'Array(Array(Variant(String, Map(String, String))))');
SELECT CAST([map('a', 'x')], 'Array(Dynamic)');
SELECT CAST([map('a', 'x')], 'Array(JSON)');
SELECT CAST([map('a', 'x')], 'Array(String)');

-- What cannot be converted is reported by the element wrapper, which names the offending types.
SELECT CAST([map('a', 'x')], 'Array(Variant(String, UInt8))'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST([map('a', 'x')], 'Array(Tuple(String, String))'); -- { serverError TYPE_MISMATCH }

-- A genuine nesting mismatch is still rejected.
SELECT CAST(['v'], 'Array(Array(String))'); -- { serverError TYPE_MISMATCH }
SELECT CAST([['v']], 'Array(String)'); -- { serverError TYPE_MISMATCH }
