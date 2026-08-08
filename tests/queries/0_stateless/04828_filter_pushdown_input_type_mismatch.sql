-- Filter push-down into `system.tables` must not use a predicate subtree whose input types
-- do not match the block it is evaluated over: `information_schema.tables` is a view where
-- `engine` is `Nullable(String)`, while `system.tables` has `engine String`. Evaluating such
-- a predicate failed with a logical error (`Unexpected return type from toString.
-- Expected Nullable(String). Got String`).
-- Found by the AST fuzzer: https://github.com/ClickHouse/ClickHouse/issues/113982

SELECT 1 FROM (SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine))) FORMAT Null;

SELECT count() > 0 FROM (SELECT 1 FROM merge('information_schema', 'TABLES') WHERE indexHint(toString(engine)));

SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine)) UNION ALL SELECT 1 FROM information_schema.tables WHERE indexHint(toString(engine)) FORMAT Null;
