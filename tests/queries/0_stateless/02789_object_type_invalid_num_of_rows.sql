set allow_experimental_object_type=1;
SELECT '0.02' GROUP BY GROUPING SETS (('6553.6'), (CAST('{"x" : 1}', 'Object(\'json\')'))) FORMAT Null; -- { serverError NOT_IMPLEMENTED }
