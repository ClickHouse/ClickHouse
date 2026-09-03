-- { echoOn }
select regexpExtract('100-200', '(\\d+)-(\\d+)', 1);
select regexpExtract('100-200', '(\\d+)-(\\d+)');
select regexpExtract('100-200', '(\\d+)-(\\d+)', 2);
select regexpExtract('100-200', '(\\d+)-(\\d+)', 0);
select regexpExtract('100-200', '(\\d+).*', 1);
select regexpExtract('100-200', '([a-z])', 1);
select regexpExtract(null, '([a-z])', 1);
select regexpExtract('100-200', null, 1);
select regexpExtract('100-200', '([a-z])', null);

select REGEXP_EXTRACT('100-200', '(\\d+)-(\\d+)', 1);
select REGEXP_EXTRACT('100-200', '(\\d+)-(\\d+)');
select REGEXP_EXTRACT('100-200', '(\\d+)-(\\d+)', 0);

select regexpExtract('0123456789', '(\d+)(\d+)', 0);
select regexpExtract('0123456789', '(\d+)(\d+)', 1);
select regexpExtract('0123456789', '(\d+)(\d+)', 2);

select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)');
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', 1);
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', 2);
select regexpExtract(materialize('100-200'), '(\\d+).*', 1);
select regexpExtract(materialize('100-200'), '([a-z])', 1);
select regexpExtract(materialize(null), '([a-z])', 1);
select regexpExtract(materialize('100-200'), null, 1);
select regexpExtract(materialize('100-200'), '([a-z])', null);

select regexpExtract('100-200', '(\\d+)-(\\d+)', materialize(1));
select regexpExtract('100-200', '(\\d+)-(\\d+)', materialize(2));
select regexpExtract('100-200', '(\\d+).*', materialize(1));
select regexpExtract('100-200', '([a-z])', materialize(1));
select regexpExtract(null, '([a-z])', materialize(1));
select regexpExtract('100-200', null, materialize(1));
select regexpExtract('100-200', '([a-z])', materialize(null));

select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', materialize(1));
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', materialize(2));
select regexpExtract(materialize('100-200'), '(\\d+).*', materialize(1));
select regexpExtract(materialize('100-200'), '([a-z])', materialize(1));
select regexpExtract(materialize(null), '([a-z])', materialize(1));
select regexpExtract(materialize('100-200'), null, materialize(1));
select regexpExtract(materialize('100-200'), '([a-z])', materialize(null));
select regexpExtract('100-200', '(\\d+)-(\\d+)', number) from numbers(3);
select regexpExtract(materialize('100-200'), '(\\d+)-(\\d+)', number) from numbers(3);
select regexpExtract(number::String || '-' || (2*number)::String, '(\\d+)-(\\d+)', 1) from numbers(3);
select regexpExtract(number::String || '-' || (2*number)::String, '(\\d+)-(\\d+)', number%3) from numbers(5);
select regexpExtract('100-200100-200', '(\\d+)-(\\d+)(\\d+)-(\\d+)', materialize(3));

select regexpExtract('100-200'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select regexpExtract('100-200', '(\\d+)-(\\d+)', 1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select regexpExtract(cast('100-200' as FixedString(10)), '(\\d+)-(\\d+)', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtract('100-200', cast('(\\d+)-(\\d+)' as FixedString(20)), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtract('100-200', '(\\d+)-(\\d+)', 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtract(100, '(\\d+)-(\\d+)', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtract('100-200', 1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select regexpExtract('100-200', materialize('(\\d+)-(\\d+)'), 1); -- { serverError ILLEGAL_COLUMN }
select regexpExtract('100-200', '(\\d+)-(\\d+)', 3); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtract('100-200', '(\\d+)-(\\d+)', -1); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
select regexpExtract('100-200', '\\d+-\\d+', 0);
select regexpExtract('100-200', '\\d+-\\d+', 1);-- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
-- { echoOff }
