-- { echoOn }
-- Two-argument form with a pattern that has no capturing group returns the whole match (group 0)
-- instead of throwing INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE.
select regexpExtract('100-200', '');
select regexpExtract('100-200', '\\d+');
select regexpExtract('100-200', '\\d+-\\d+');
select regexpExtract(materialize('100-200'), '\\d+');
select regexpExtract(materialize('100-200'), '');
select regexpExtract(number::String, '\\d+') from numbers(3);

-- Explicit index is still honored: a pattern with no group only has group 0, index 1 is out of range.
select regexpExtract('100-200', '\\d+', 0);
select regexpExtract('100-200', '\\d+', 1); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }

-- A pattern with a capturing group keeps the default index 1 (first group).
select regexpExtract('100-200', '(\\d+)-(\\d+)');
-- { echoOff }
