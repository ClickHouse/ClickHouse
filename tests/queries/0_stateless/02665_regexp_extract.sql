select regexpExtract('100-200', '(\\d+)-(\\d+)', 1);
select regexpExtract('100-200', '(\\d+)-(\\d+)');
select regexpExtract('100-200', '(\\d+)-(\\d+)', 2);
select regexpExtract('100-200', '(\\d+).*', 1);
select regexpExtract('100-200', '([a-z])', 1);
select regexpExtract(null, '([a-z])', 1);
select regexpExtract('100-200', null, 1);
select regexpExtract('100-200', '([a-z])', null);

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


select regexpExtract('100-200'); -- { serverError 42 }
select regexpExtract('100-200', '(\\d+)-(\\d+)', 1, 2); -- { serverError 42 }
select regexpExtract(cast('100-200' as FixedString(10)), '(\\d+)-(\\d+)', 1); -- { serverError 43 }
select regexpExtract('100-200', cast('(\\d+)-(\\d+)' as FixedString(20)), 1); -- { serverError 43 }
select regexpExtract('100-200', materialize('(\\d+)-(\\d+)'), 1); -- { serverError 44 }
