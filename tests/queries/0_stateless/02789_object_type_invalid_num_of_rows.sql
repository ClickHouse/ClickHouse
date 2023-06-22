set allow_experimental_object_type=1;
SELECT '0.02' GROUP BY GROUPING SETS (('6553.6'), (CAST(CAST('{"x" : 1}', 'Object(Nullable(\'json\'))'), 'Object(\'json\')'))) settings max_threads=1; -- { serverError NOT_IMPLEMENTED }
