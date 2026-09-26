-- A NULL in an exact CAST literal must occur at the same array nesting
-- depth as the Nullable target that reads it.

SET input_format_null_as_default = 1;

-- Baseline: the ordinary CAST path already rejects this shape.
SELECT CAST([[1], NULL] AS Array(Array(Nullable(Int64)))); -- { serverError TYPE_MISMATCH }

-- Wrong NULL nesting with a wide integer using the exact literal path.
SELECT CAST([[1], NULL] AS Array(Array(Nullable(Int128)))); -- { serverError TYPE_MISMATCH }

-- All CAST syntaxes must behave consistently.
SELECT CAST([[1], NULL], 'Array(Array(Nullable(Int128)))'); -- { serverError TYPE_MISMATCH }
SELECT [[1], NULL]::Array(Array(Nullable(Int128))); -- { serverError TYPE_MISMATCH }

-- Exercise the other exact numeral readers.
SELECT CAST([[1], NULL] AS Array(Array(Nullable(UInt256)))); -- { serverError TYPE_MISMATCH }
SELECT CAST([[1.5], NULL] AS Array(Array(Nullable(Decimal(10, 2))))); -- { serverError TYPE_MISMATCH }

-- NULL at exactly the Nullable depth is valid.
SELECT CAST([[1], [NULL]] AS Array(Array(Nullable(Int128))));
SELECT CAST([[NULL, NULL], [1, NULL]] AS Array(Array(Nullable(Int128))));

-- One valid NULL must not hide another NULL at an invalid depth.
SELECT CAST([[NULL], NULL] AS Array(Array(Nullable(Int128)))); -- { serverError TYPE_MISMATCH }

-- Verify deeper array nesting.
SELECT CAST([[[1]], [NULL]] AS Array(Array(Array(Nullable(Int128))))); -- { serverError TYPE_MISMATCH }
SELECT CAST([[[1]], [[NULL]]] AS Array(Array(Array(Nullable(Int128)))));
