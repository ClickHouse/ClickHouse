-- An element of a floating-point array is in an intersection whose element type is an integer
-- only if its value is an integer that fits that type. Likewise, an element of a Decimal array
-- cast to a lower scale is in the intersection only if the cast keeps its value.

SELECT arrayIntersect([1.5, 2], [1, 2]);
SELECT arrayIntersect(CAST([1.5, 2] AS Array(Float32)), CAST([1, 2] AS Array(UInt8))),
       arrayIntersect(CAST([1, 2] AS Array(UInt8)), CAST([1.5, 2] AS Array(Float32)));
SELECT arrayIntersect(CAST([300.5, 2] AS Array(Float64)), CAST([44, 2] AS Array(UInt8)));
SELECT arrayIntersect(CAST([-0.5, 7] AS Array(Float32)), CAST([0, 7] AS Array(UInt8)));
-- the floating-point array is the longer one, then the shorter one
SELECT arrayIntersect(CAST([1.5, 2, 1] AS Array(Float32)), CAST([1, 2] AS Array(UInt8)));
SELECT arrayIntersect(CAST([1.5, 3] AS Array(Float32)), CAST([1, 2, 3, 4] AS Array(UInt8)));
SELECT arrayIntersect(a, b) FROM values('a Array(Float64), b Array(UInt8)', ([1.5, 2], [1, 2]), ([256, 5], [0, 5]), ([2.0], [2]));
SELECT arrayIntersect([1.5, 2, NULL], [1, 2]), arrayIntersect([1.5, 2, NULL], [1, 2, NULL]);
SELECT arrayIntersect([toBFloat16(1.5), toBFloat16(2)], [1, 2]);
SELECT arrayIntersect([toLowCardinality(1.5), toLowCardinality(2.)], [toUInt8(1), toUInt8(2)]);
SELECT arrayIntersect(CAST([1.5, 2] AS Array(Float64)), CAST([1, 2] AS Array(Int128)));
SELECT round(arrayJaccardIndex([1.5, 2], [1, 2]), 2);
-- an integer value that wraps, a value out of the range of the type, and `Bool`
SELECT arrayIntersect([257.0], CAST([1] AS Array(UInt8)));
SELECT arrayIntersect([1e20], CAST([-9223372036854775808, 9223372036854775807] AS Array(Int64)));
SELECT arrayIntersect([1.5], [true]), arrayIntersect([true], [1.5]);

-- a Decimal cast to a lower scale
SELECT arrayIntersect([toDecimal32(1.55, 2)], [toDecimal32(1.5, 1), toDecimal32(1.6, 1)]);
SELECT arrayIntersect(materialize([toDecimal64(1.55, 2), toDecimal64(1.6, 2)]), [toDecimal32(1.5, 1), toDecimal32(1.6, 1)]);
SELECT arrayIntersect([toDecimal256(1.12, 2)], [toDecimal256(1.1, 1)]);

-- unchanged: a float that is an integer (also -0) is a member
SELECT arrayIntersect([1.0, 2.0, -0.0], [0, 1, 2]);
-- unchanged: a Decimal whose value the lower scale keeps is a member
SELECT arrayIntersect([toDecimal32(1.50, 2), toDecimal32(1.55, 2)], [toDecimal32(1.5, 1), toDecimal32(1.6, 1)]);
-- unchanged: a cast to a narrower floating-point type rounds, and the rounded value is a member
SELECT arrayIntersect([0.1, 0.2, 0.3], CAST([0.2, 0.3] AS Array(Float32)));
-- unchanged: union and symmetric difference do not narrow
SELECT arraySort(arrayUnion([1.5, 2], [1, 2])), arraySort(arraySymmetricDifference([1.5, 2], [1, 2]));
SELECT arraySort(arrayUnion([toDecimal32(1.55, 2)], [toDecimal32(1.5, 1)]));
