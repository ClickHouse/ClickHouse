-- arrayLevenshtein
CREATE TABLE simple_levenshtein (lhs Array(UInt8), rhs Array(UInt8)) ENGINE MergeTree ORDER BY tuple();
INSERT INTO simple_levenshtein VALUES
  ([1, 2, 3, 4], [1, 2, 3, 4]),
  ([1, 2, 3, 4], [1, 3, 3, 4]),
  ([1, 2, 3, 4], [1, 3, 2, 4]),
  ([1, 4], [1, 2, 3, 4]),
  ([1, 2, 3, 4], []),
  ([], [1, 3, 2, 4]),
  ([], []);
SELECT arrayLevenshtein(lhs, rhs) FROM simple_levenshtein;
SELECT '';

-- arrayLevenshteinWeighted
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
SELECT arrayLevenshteinWeighted(lhs, rhs, lhs_weights, rhs_weights) FROM weighted_levenshtein;
SELECT '';

-- arraySimilarity
SELECT round(arraySimilarity(lhs, rhs, lhs_weights, rhs_weights), 5) FROM weighted_levenshtein;

-- errors NUMBER_OF_ARGUMENTS_DOESNT_MATCH
SELECT arrayLevenshtein(lhs, rhs, lhs_weights, rhs_weights) FROM weighted_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayLevenshteinWeighted(lhs, rhs) FROM simple_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT round(arraySimilarity(lhs, rhs), 5) FROM simple_levenshtein; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- errors ILLEGAL_TYPE_OF_ARGUMENT
SELECT arrayLevenshtein(1, [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayLevenshteinWeighted([1], 1, [1, 2], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySimilarity([1, 2], 1, [1, 2], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
--
SELECT arrayLevenshteinWeighted([1, 2], [1], [1., 2], [1., 2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT arraySimilarity([1, 2], [1], [1., 2], [1., 2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
