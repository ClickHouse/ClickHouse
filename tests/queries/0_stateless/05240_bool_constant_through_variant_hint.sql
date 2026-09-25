-- A `Bool` read from a column is a `UInt64` field, while a `Bool` literal is a `Bool` one, and a `String` target
-- prints the tag. A `Variant` hint resolved to `Bool` gives the field its tag back, in containers too.
SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

SELECT * FROM values('x String', CAST(true, 'Variant(Bool, String)'));
SELECT * FROM values('x Array(String)', [CAST(true, 'Variant(Bool, String)'), CAST(false, 'Variant(Bool, String)')]);
SELECT * FROM values('x Map(String, String)', map('k', CAST(true, 'Variant(Bool, String)')));
SELECT * FROM values('x String', CAST(true, 'Bool'));
SELECT * FROM values('x UInt8', CAST(true, 'Variant(Bool, String)'));
-- A scalar constant reaches the conversion with its active alternative already resolved from the column.
SELECT * FROM values('x String', CAST(true, 'Variant(Bool, UInt8)'));
