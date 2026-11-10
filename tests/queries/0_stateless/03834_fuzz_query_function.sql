-- Tags: no-fasttest
SET allow_fuzz_query_functions = 1;
SELECT length(fuzzQuery('SELECT 1')) > 0;
SELECT fuzzQuery('not a valid query'); -- { serverError SYNTAX_ERROR }
