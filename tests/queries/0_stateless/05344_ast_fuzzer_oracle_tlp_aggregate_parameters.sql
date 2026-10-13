-- Tags: no-fasttest
-- no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
--              are not allowed when `allow_feature_tier=0` (the Fast test default).
--
-- The TLP Aggregate oracle compares `agg(params)(args)` with `aggMerge(params)(_s)` over
-- `aggState(params)(args) AS _s` computed per WHERE partition. The `-Merge` call must keep the
-- parameters: a quantile state does not store its level, so a parameterless `-Merge` evaluated the
-- default level and the oracle raised a false `AST_FUZZER_ORACLE_MISMATCH`. The plural form checks
-- that the whole parameter list is kept. Repeat the queries to make a regression practically certain
-- to fire.

DROP TABLE IF EXISTS oracle_tlp_agg_params;
CREATE TABLE oracle_tlp_agg_params (v Int64, w UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO oracle_tlp_agg_params SELECT number, number % 3 + 1 FROM numbers(100);

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 1;
SET ast_fuzzer_oracle = 1;

SELECT quantileExactWeightedInterpolated(0.)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantileExactWeightedInterpolated(0.)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantileExactWeightedInterpolated(0.)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantileExactWeightedInterpolated(0.)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantileExactWeightedInterpolated(0.)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, 0.9)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, 0.9)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, 0.9)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, 0.9)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, 0.9)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;

-- `*` in a parameter expands to other columns in the rewritten outer query, so the oracle must skip it.
SELECT quantilesExactWeightedInterpolated(0.1, * APPLY isNull)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, * APPLY isNull)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, * APPLY isNull)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, * APPLY isNull)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;
SELECT quantilesExactWeightedInterpolated(0.1, * APPLY isNull)(v, w) FROM oracle_tlp_agg_params WHERE v > 10;

DROP TABLE oracle_tlp_agg_params;
