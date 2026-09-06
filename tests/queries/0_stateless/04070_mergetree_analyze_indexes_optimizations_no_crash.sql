-- Arguments of the `vector_search_index_analysis` optimization are evaluated as expressions, so a
-- malformed argument is rejected with `BAD_ARGUMENTS` instead of reaching `IAST::getColumnName`.
DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int, value Int) ENGINE = MergeTree() ORDER BY key;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', tuple(materialize(1))); -- { serverError BAD_ARGUMENTS }

-- A subquery argument, which resolves to an expression list rather than a column.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- An argument node with no children.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', 1); -- { serverError BAD_ARGUMENTS }

-- `mergeTreeAnalyzeIndexesUUID` reaches the same code at a different argument index. Arguments are
-- parsed before the table is resolved, so the UUID does not have to exist.
SELECT * FROM mergeTreeAnalyzeIndexesUUID('00000000-0000-0000-0000-000000000001', 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- `_CAST` takes a value and a type name. Any other argument list is not a cast wrapper, in the
-- optimization name and in the parts array alike. The analyzer rejects such a `_CAST` while
-- resolving it, so the code differs per analyzer.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], _CAST(), [1]); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST()); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST(['all_1_1_0'])); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST(['all_1_1_0'], 'Array(String)', 'extra')); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A parts argument that is not an array, and arrays whose elements are not strings. The literal and
-- the `array(...)` spellings reach different branches.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST(1, 'UInt8')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, _CAST([1], 'Array(UInt8)')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, array(1)); -- { serverError BAD_ARGUMENTS }

-- A well-formed parts array is still accepted. `data` has no parts, so nothing is returned.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, array('all_1_1_0'));

-- A well-formed constant array is still accepted and reaches the arity check.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', CAST([1, 2, 3] AS Array(UInt8))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The heterogeneous array needs `Variant` as its common type. Pin the setting: the stress job
-- randomizes `compatibility`, and a version below 26.1 restores this setting's old default of 0.
SET use_variant_as_common_type = 1;

-- A six-element array is decoded element by element, each with its own expected type. `data` has no
-- parts, so the analysis itself returns nothing.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, [1.0], false, false));

-- The same six-element array behind `_CAST` optimization names of one, two and three arguments.
-- Everything except the name is valid here, so the two-argument name must be unwrapped and succeed
-- while the other two must fail, rather than reaching a second accepted error code.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], _CAST('vector_search_index_analysis'), array('value', 'L2Distance', 1, [1.0], false, false)); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], _CAST('vector_search_index_analysis', 'String'), array('value', 'L2Distance', 1, [1.0], false, false));
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], _CAST('vector_search_index_analysis', 'String', 'extra'), array('value', 'L2Distance', 1, [1.0], false, false)); -- { serverError BAD_ARGUMENTS, NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Each element of a six-element array has its own expected type, and an element of the wrong type is
-- reported as `BAD_ARGUMENTS` rather than as a failed `Field` access. The remaining five elements are
-- valid in every case below, so an element that stopped being checked would return an empty result
-- instead of a second accepted error code.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array(1, 'L2Distance', 1, [1.0], false, false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 1, 1, [1.0], false, false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 'one', [1.0], false, false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, 'one', false, false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, ['one'], false, false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, [1.0], 'no', false)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, [1.0], false, 'no')); -- { serverError BAD_ARGUMENTS }

-- A search vector of integers is accepted, as the optimization itself accepts one. A `Float64` with
-- an integral value is formatted without a decimal point, so this is also the text the server sends
-- to itself when it distributes the analysis of such a search vector.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, [1, 2, 3], false, false));

DROP TABLE data;
