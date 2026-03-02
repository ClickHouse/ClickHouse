-- untuple is not available without analyzer
SET enable_analyzer=1;

CREATE TABLE test Engine=Merge(default, untuple((1,1))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Fuzzed
DESCRIBE TABLE format(untuple((toIntervalQuarter(1), assumeNotNull(8) IS NOT NULL, isNullable(7) % 1, 4, materialize('hola'), 4)), JSONEachRow)
    FINAL
    SETTINGS schema_inference_hints = 'ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼ð(Œ¼';  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
DESCRIBE TABLE format(untuple(((SELECT DISTINCT 1 GROUP BY or(isNullable(1048576), 1), or(1048576, 1 AS x, 10, isNull(isNullable(1))) WITH ROLLUP), 1)), JSONEachRow);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
