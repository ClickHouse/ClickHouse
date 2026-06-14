-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

-- Type mismatches are rejected by the declarative signature with ILLEGAL_TYPE_OF_ARGUMENT.
SELECT naiveBayesClassifier('sentiment', 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT naiveBayesClassifier(0, 'hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT naiveBayesClassifier('zzz_nonexistent_model_4ae239f8', 'hello'); -- { serverError BAD_ARGUMENTS }

-- Empty input not allowed
SELECT naiveBayesClassifier('lang_byte_2', '');  -- { serverError BAD_ARGUMENTS }
