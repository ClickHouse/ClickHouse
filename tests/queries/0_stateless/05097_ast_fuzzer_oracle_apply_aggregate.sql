-- Tags: no-fasttest
-- no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
--              are not allowed when `allow_feature_tier=0` (the Fast test default).
--
-- `SELECT * APPLY any` aggregates every expanded column, but the aggregate is not an
-- `ASTFunction` in the AST: the transformer keeps only the function name (or a lambda).
-- `QueryOracleChecker::hasAggregates` used to miss it, so such a query reached the
-- non-aggregate TLP / DISTINCT oracles, where the partition over an always-false WHERE
-- still yields the one row every aggregate returns on empty input, and the oracle raised
-- a false `AST_FUZZER_ORACLE_MISMATCH`. Now the transformer counts as an aggregate:
-- the query is skipped by the non-aggregate oracles and rejected by the aggregate one.
-- The fuzzer preserves the topmost WHERE and SELECT list shape in oracle mode, so the
-- shape below reaches the oracle in the vast majority of runs; repeat to make a
-- regression practically certain to fire.

DROP TABLE IF EXISTS oracle_apply_agg;
CREATE TABLE oracle_apply_agg (i Int32, d Date) ENGINE = MergeTree ORDER BY i;
INSERT INTO oracle_apply_agg VALUES (1, '2020-01-01'), (2, '2020-01-02'), (3, '2020-01-03');

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 1;
SET ast_fuzzer_oracle = 1;

SELECT DISTINCT a.* APPLY any FROM oracle_apply_agg AS a WHERE 100;
SELECT DISTINCT a.* APPLY any FROM oracle_apply_agg AS a WHERE 100;
SELECT DISTINCT a.* APPLY any FROM oracle_apply_agg AS a WHERE 100;
SELECT DISTINCT a.* APPLY any FROM oracle_apply_agg AS a WHERE 100;
SELECT DISTINCT a.* APPLY any FROM oracle_apply_agg AS a WHERE 100;
SELECT * APPLY max FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY max FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY max FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY (x -> min(x)) FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY (x -> min(x)) FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY (x -> min(x)) FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY minOrNull FROM oracle_apply_agg WHERE i > 1;
SELECT * APPLY minOrNull FROM oracle_apply_agg WHERE i > 1;

-- A non-aggregate APPLY is still eligible for the oracles and must keep passing them.
SELECT * APPLY toString FROM oracle_apply_agg WHERE i > 1 ORDER BY i;
SELECT * APPLY toString FROM oracle_apply_agg WHERE i > 1 ORDER BY i;

DROP TABLE oracle_apply_agg;
