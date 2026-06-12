-- { echoOn }
-- Out-of-range index through the vectorConstant path: non-const haystack + const index.
-- (The const-haystack form folds into a different internal path via default-impl-for-constants.)
select regexpExtract(materialize('100-200'), '\\d+', 1); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtract(materialize('100-200'), '\\d+', -1); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', 3); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
-- A valid in-range index on the same path still returns the match.
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', 0);
select regexpExtract(materialize('100-200'), '\\d+', 0);
-- { echoOff }
