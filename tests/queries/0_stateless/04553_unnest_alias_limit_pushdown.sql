-- Defense-in-depth guard for https://github.com/ClickHouse/ClickHouse/pull/110188
-- Companion to 04321 (which covers `arrayJoin` written by its canonical name).
--
-- `unnest` is a case-insensitive alias of `arrayJoin` (`src/Functions/array/arrayJoin.cpp`).
-- The guards that reject pushing a `LIMIT` into a source below a row-changing `arrayJoin`
-- (`numbersLikeUtils::astContainsArrayJoinFunction`, `selectListHasArrayJoinFunction`, and the
-- planner `hasArrayJoinFunctionNode`) canonicalize the function name, so the alias is treated
-- the same as `arrayJoin` even when function names are not normalized
-- (`normalize_function_names = 0`). Otherwise the source could be capped to `limit` rows before
-- the expansion runs, and an empty-array prefix would produce too few output rows.
--
-- (Function-name resolution already rewrites `unnest` to `arrayJoin` in the query tree, so the
-- checks would fire even without canonicalization today; comparing canonically makes the guards
-- self-sufficient regardless of how the name reaches them. `unnest` under
-- `normalize_function_names = 0` is only usable with the analyzer -- the old interpreter cannot
-- execute the alias directly.)
--
-- Rows 0..2 expand to empty arrays via `if`, so the first three non-empty values (3, 4, 5) must
-- be returned regardless of `normalize_function_names`.

SELECT '-- unnest in SELECT, normalize_function_names = 0';
SELECT unnest(if(number < 3, [], [number])) FROM numbers(100) LIMIT 3
    SETTINGS enable_analyzer = 1, normalize_function_names = 0;
SELECT '-- unnest in SELECT, optimizer disabled, normalize_function_names = 0';
SELECT unnest(if(number < 3, [], [number])) FROM numbers(100) LIMIT 3
    SETTINGS enable_analyzer = 1, query_plan_enable_optimizations = 0, normalize_function_names = 0;
SELECT '-- unnest via WITH alias in WHERE only, normalize_function_names = 0';
WITH unnest(if(number < 3, [], [number])) AS x SELECT number FROM numbers(100) WHERE x >= 0 LIMIT 3
    SETTINGS enable_analyzer = 1, normalize_function_names = 0;
SELECT '-- unnest in SELECT, name normalization on';
SELECT unnest(if(number < 3, [], [number])) FROM numbers(100) LIMIT 3
    SETTINGS enable_analyzer = 1, normalize_function_names = 1;
