-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

SELECT naiveBayesClassifier('sentiment', 3); -- { serverError BAD_ARGUMENTS }

SELECT naiveBayesClassifier(0, 'hello'); -- { serverError BAD_ARGUMENTS }

SELECT naiveBayesClassifier('zzz_nonexistent_model_4ae239f8', 'hello'); -- { serverError BAD_ARGUMENTS }

-- Empty input not allowed
SELECT naiveBayesClassifier('lang_byte_2', '');  -- { serverError BAD_ARGUMENTS }
