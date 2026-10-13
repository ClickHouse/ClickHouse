-- Tags: no-fasttest, no-parallel
-- no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
--              are not allowed when `allow_feature_tier=0` (the Fast test default).
--
-- Smoke coverage for `QueryOracleChecker::checkTLPAggregate` so the dedicated
-- aggregate-oracle rewrite path (State/Merge + WHERE -> UNION ALL) doesn't
-- regress unnoticed. We feed deterministic aggregates with and without
-- GROUP BY; the oracle's metamorphic equality check must hold, so the
-- queries succeed with `ast_fuzzer_oracle=1` exactly as without it.

DROP TABLE IF EXISTS oracle_tlp_agg;
CREATE TABLE oracle_tlp_agg (g UInt8, v Int64) ENGINE = MergeTree ORDER BY g;
INSERT INTO oracle_tlp_agg SELECT number % 5, number FROM numbers(200);

-- Suppress error-level log messages from fuzzed queries that fail expectedly
-- (matches 03833_server_ast_fuzzer.sql). Without this the `flaky check` job
-- reruns this test 100x and trips on `<Error>` log lines from random AST
-- mutations that produce syntactically valid but semantically nonsense
-- queries (e.g. `min` with wrong arity, FINAL on MergeTree).
SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 1;
SET ast_fuzzer_oracle = 1;

-- Aggregate without GROUP BY.
SELECT count(), min(v), max(v) FROM oracle_tlp_agg WHERE v > 50;

-- Aggregate with GROUP BY.
SELECT g, count(), min(v), max(v) FROM oracle_tlp_agg WHERE v > 50 GROUP BY g ORDER BY g;

-- Aggregate with GROUP BY producing more groups than rows passing WHERE
-- (covers the State/Merge passthrough for the partition where predicate is false).
SELECT g, count() FROM oracle_tlp_agg WHERE v >= 0 GROUP BY g ORDER BY g;

DROP TABLE oracle_tlp_agg;
