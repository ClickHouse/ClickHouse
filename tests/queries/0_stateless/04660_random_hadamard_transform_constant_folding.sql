-- `randomHadamardTransform` of a constant vector is a constant: it must be evaluated once, not once per row.

-- The call is folded to a literal, so it is not repeated for every row of the block.
SELECT isConstant(randomHadamardTransform([1, 2, 3, 4]::Array(Float32)));
SELECT isConstant(randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 42));
SELECT isConstant(randomHadamardTransform(CAST(range(16), 'Array(Float32)'), 7, 5));

-- A non-constant vector is still transformed row by row.
SELECT isConstant(randomHadamardTransform(materialize([1, 2, 3, 4]::Array(Float32))));

-- The constant and the per-row paths agree, for every element type and for every optional argument.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32)) = randomHadamardTransform(materialize([1, 2, 3, 4]::Array(Float32))),
       randomHadamardTransform([1, 2, 3, 4]::Array(Float64), 42) = randomHadamardTransform(materialize([1, 2, 3, 4]::Array(Float64)), 42),
       randomHadamardTransform([1, 2, 3, 4]::Array(BFloat16), 42) = randomHadamardTransform(materialize([1, 2, 3, 4]::Array(BFloat16)), 42),
       randomHadamardTransform(CAST(range(16), 'Array(Float32)'), 7, 5) = randomHadamardTransform(materialize(CAST(range(16), 'Array(Float32)')), 7, 5);

-- A per-row transform of vectors of different lengths keeps working.
SELECT length(randomHadamardTransform(CAST(range(number + 1), 'Array(Float32)'), 0, 1)) FROM numbers(5);

-- 'seed' and 'output_dims' must be constant even when the vector is not.
SELECT randomHadamardTransform(materialize([1, 2]::Array(Float32)), materialize(1)); -- { serverError ILLEGAL_COLUMN }
SELECT randomHadamardTransform(materialize([1, 2]::Array(Float32)), 0, materialize(1)); -- { serverError ILLEGAL_COLUMN }

-- Argument validation is unaffected by the constant path.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 0, 8); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomHadamardTransform([]::Array(Float32), 0, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- An empty vector yields an empty array and 'output_dims' does not apply to it, in both paths.
SELECT randomHadamardTransform([]::Array(Float32)), randomHadamardTransform(materialize([]::Array(Float32)));
SELECT randomHadamardTransform([]::Array(Float32), 0, 1), randomHadamardTransform(materialize([]::Array(Float32)), 0, 1);
