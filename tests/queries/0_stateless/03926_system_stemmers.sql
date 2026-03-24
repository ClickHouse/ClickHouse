-- Tags: no-fasttest
-- Tag no-fasttest: depends on libstemmer_c

SET allow_experimental_nlp_functions = 0;
SELECT * FROM system.stemmers; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_nlp_functions = 1;
SELECT * FROM system.stemmers ORDER BY ALL;
