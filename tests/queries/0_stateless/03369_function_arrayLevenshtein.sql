-- arrayLevenshteinDistance
CREATE TABLE simple_levenshtein (lhs Array(UInt8), rhs Array(UInt8)) ENGINE MergeTree ORDER BY tuple();
INSERT INTO simple_levenshtein VALUES
  ([1, 2, 3, 4], [1, 2, 3, 4]),
  ([1, 2, 3, 4], [1, 3, 3, 4]),
  ([1, 2, 3, 4], [1, 3, 2, 4]),
  ([1, 4], [1, 2, 3, 4]),
  ([1, 2, 3, 4], []),
  ([], [1, 3, 2, 4]),
  ([], []);
SELECT arrayLevenshteinDistance(lhs, rhs) FROM simple_levenshtein;
SELECT '';

-- arrayLevenshteinDistance for different types
SELECT arrayLevenshteinDistance(['1', '2'], ['1']),
  arrayLevenshteinDistance([toFixedString('1', 16), toFixedString('2', 16)], [toFixedString('1', 16)]),
  arrayLevenshteinDistance([toUInt16(1)], [toUInt16(2), 1]),
  arrayLevenshteinDistance([toFloat32(1.1), 2], [toFloat32(1.1)]),
  arrayLevenshteinDistance([toFloat64(1.1), 2], [toFloat64(1.1)]),
  arrayLevenshteinDistance([toDate('2025-01-01'), toDate('2025-01-02')], [toDate('2025-01-01')]);
SELECT '';

-- arrayLevenshteinDistanceWeighted
CREATE TABLE weighted_levenshtein (lhs Array(String), rhs Array(String), lhs_weights Array(Float64), rhs_weights Array(Float64)) ENGINE MergeTree ORDER BY tuple();
INSERT INTO weighted_levenshtein VALUES
  (['A', 'B', 'C'], ['A', 'C'], [1, 2, 3], [1, 3]),
  (['A', 'C'], ['A', 'B', 'C'], [1, 3], [1, 2, 3]),
  (['A', 'B'], ['A', 'C'], [1, 2], [3, 4]),
  (['A', 'B', 'C'], ['A', 'K', 'L'], [1, 2, 3], [3, 4, 5]),
  ([], [], [], []),
  (['A', 'B'], [], [1, 2], []),
  (['A', 'B'], ['A', 'B'], [1, 2], [2, 1]),
  (['A', 'B'], ['C', 'D'], [1, 2], [3, 4]),
  (['A', 'B', 'C'], ['C', 'B', 'A'], [1, 2, 3], [4, 5, 6]),
  (['A', 'B'], ['C', 'A', 'B'], [1, 2], [4, 5, 6]),
  (['A', 'B', 'C', 'D', 'E', 'F', 'G'], ['A', 'B', 'X', 'D', 'E', 'Y', 'G'], [1, 1, 1, 1, 1, 1, 1], [1, 1, 1, 1, 1, 1, 1]);
SELECT arrayLevenshteinDistanceWeighted(lhs, rhs, lhs_weights, rhs_weights) FROM weighted_levenshtein;
SELECT '';

-- arrayLevenshteinDistance for different types
SELECT arrayLevenshteinDistanceWeighted(['1', '2'], ['1'], [1., 2], [1.]),
  arrayLevenshteinDistanceWeighted([toFixedString('1', 16), toFixedString('2', 16)], [toFixedString('1', 16)], [1., 2], [1.]),
  arrayLevenshteinDistanceWeighted([toUInt16(1)], [toUInt16(2), 1], [1.], [2., 1]),
  arrayLevenshteinDistanceWeighted([toFloat32(1.1), 2], [toFloat32(1.1)], [1., 2], [1.]),
  arrayLevenshteinDistanceWeighted([toFloat64(1.1), 2], [toFloat64(1.1)], [1., 2], [1.]),
  arrayLevenshteinDistanceWeighted([toDate('2025-01-01'), toDate('2025-01-02')], [toDate('2025-01-01')], [1., 2], [1.]);
SELECT '';

-- arraySimilarity
SELECT round(arraySimilarity(lhs, rhs, lhs_weights, rhs_weights), 5) FROM weighted_levenshtein;

-- errors NUMBER_OF_ARGUMENTS_DOESNT_MATCH
SELECT arrayLevenshteinDistance(lhs, rhs, lhs_weights, rhs_weights) FROM weighted_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayLevenshteinDistanceWeighted(lhs, rhs) FROM simple_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT round(arraySimilarity(lhs, rhs), 5) FROM simple_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- errors ILLEGAL_TYPE_OF_ARGUMENT
SELECT arrayLevenshteinDistance(1, [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayLevenshteinDistanceWeighted([1], 1, [1, 2], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySimilarity([1, 2], 1, [1, 2], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
--
SELECT arrayLevenshteinDistanceWeighted([1, 2], [1], [1., 2], [1., 2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT arraySimilarity([1, 2], [1], [1., 2], [1., 2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
