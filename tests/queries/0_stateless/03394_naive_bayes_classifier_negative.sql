-- Argument validation rejects a non-String input or a non-String dictionary name before any dictionary is
-- resolved. The input text must be String, not FixedString, matching the dictionary key type. A dictionary
-- name that does not resolve is a bad argument.
SELECT naiveBayesClassifier('sentiment', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifier('sentiment', toFixedString('x', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifier(0, 'hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT naiveBayesClassifier('zzz_nonexistent_model_4ae239f8', 'hello'); -- { serverError BAD_ARGUMENTS }
