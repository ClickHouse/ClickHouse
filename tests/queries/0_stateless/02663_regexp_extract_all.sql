-- { echoOn }
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 1);
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)');
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 2);
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 0);
select regexpExtractAll('100-200 300-400', '(\\d+).*', 1);
select regexpExtractAll('100-200 300-400', '([a-z])', 1);
select regexpExtractAll(null, '([a-z])', 1);
select regexpExtractAll('100-200 300-400', null, 1);
select regexpExtractAll('100-200 300-400', '([a-z])', null);

select REGEXP_EXTRACT_ALL('100-200 300-400', '(\\d+)-(\\d+)', 1);
select REGEXP_EXTRACT_ALL('100-200 300-400', '(\\d+)-(\\d+)');
select REGEXP_EXTRACT_ALL('100-200 300-400', '(\\d+)-(\\d+)', 0);

select regexpExtractAll('0123456789', '(\d+)(\d+)', 0);
select regexpExtractAll('0123456789', '(\d+)(\d+)', 1);
select regexpExtractAll('0123456789', '(\d+)(\d+)', 2);

select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)');
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)', 1);
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)', 2);
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+).*', 1);
select regexpExtractAll(materialize('100-200 300-400'), '([a-z])', 1);
select regexpExtractAll(materialize(null), '([a-z])', 1);
select regexpExtractAll(materialize('100-200 300-400'), null, 1);
select regexpExtractAll(materialize('100-200 300-400'), '([a-z])', null);

select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', materialize(1));
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', materialize(2));
select regexpExtractAll('100-200 300-400', '(\\d+).*', materialize(1));
select regexpExtractAll('100-200 300-400', '([a-z])', materialize(1));
select regexpExtractAll(null, '([a-z])', materialize(1));
select regexpExtractAll('100-200 300-400', null, materialize(1));
select regexpExtractAll('100-200 300-400', '([a-z])', materialize(null));

select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)', materialize(1));
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)', materialize(2));
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+).*', materialize(1));
select regexpExtractAll(materialize('100-200 300-400'), '([a-z])', materialize(1));
select regexpExtractAll(materialize(null), '([a-z])', materialize(1));
select regexpExtractAll(materialize('100-200 300-400'), null, materialize(1));
select regexpExtractAll(materialize('100-200 300-400'), '([a-z])', materialize(null));
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', number) from numbers(3);
select regexpExtractAll(materialize('100-200 300-400'), '(\\d+)-(\\d+)', number) from numbers(3);
select regexpExtractAll(number::String || '-' || (2*number)::String, '(\\d+)-(\\d+)', 1) from numbers(3);
select regexpExtractAll(number::String || '-' || (2*number)::String, '(\\d+)-(\\d+)', number%3) from numbers(5);
select regexpExtractAll('100-200100-200 300-400300-400', '(\\d+)-(\\d+)(\\d+)-(\\d+)', materialize(3));

select regexpExtractAll('100-200 300-400', '\\d+-\\d+', 0);

select regexpExtractAll('100-200 300-400'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select regexpExtractAll(cast('100-200 300-400' as FixedString(20)), '(\\d+)-(\\d+)', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtractAll('100-200 300-400', cast('(\\d+)-(\\d+)' as FixedString(20)), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtractAll(100, '(\\d+)-(\\d+)', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtractAll('100-200 300-400', 1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtractAll('100-200 300-400', materialize('(\\d+)-(\\d+)'), 1); -- { serverError ILLEGAL_COLUMN }
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', 3); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtractAll('100-200 300-400', '(\\d+)-(\\d+)', -1); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtractAll('100-200 300-400', '\\d+-\\d+', 1);-- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
-- { echoOff }
