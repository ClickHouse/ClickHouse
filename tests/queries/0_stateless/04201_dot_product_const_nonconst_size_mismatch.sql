-- Array size mismatch must be rejected when one argument is a constant.
SELECT dotProduct([1.0::Float32, 2.0::Float32], vec)
FROM (SELECT materialize([1.0, 2.0, 3.0, 4.0]::Array(Float32)) AS vec); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
