-- Tags: no-fasttest
-- no-fasttest: SET ast_fuzzer_runs / ast_fuzzer_oracle are EXPERIMENTAL-tier settings and
--              are not allowed when `allow_feature_tier=0` (the Fast test default).
--
-- An alias defined in `WHERE` and used inside an `APPLY` lambda must not make the fuzzer's
-- correctness oracle report a mismatch: the oracle compares the query against a variant with the
-- `WHERE` removed, and without it the name `a` is the table column instead of the alias, so the
-- two are not comparable. Column `a` below deliberately does not equal `i * 100`, which is what
-- makes the rebinding change the rows rather than merely the spelling. Repeated because each
-- statement is fuzzed once and the mutation can drop the shape.

DROP TABLE IF EXISTS oracle_apply_alias;
CREATE TABLE oracle_apply_alias (i Int32, a Int32) ENGINE = MergeTree ORDER BY i;
INSERT INTO oracle_apply_alias VALUES (1, 100), (2, 201), (3, 302);

SET send_logs_level = 'fatal';
SET ast_fuzzer_runs = 1;
SET ast_fuzzer_oracle = 1;

SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;
SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;
SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;
SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;
SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;
SELECT * APPLY (x -> toInt32(x) + a) FROM oracle_apply_alias WHERE (i * 100 AS a) > 1 ORDER BY i;

-- The same lambda without the alias stays eligible for the oracle, so the assertion above is the
-- guard refusing this query and not the oracle declining the shape.
SELECT * APPLY (x -> toInt32(x) + 1) FROM oracle_apply_alias WHERE i > 1 ORDER BY i;

DROP TABLE oracle_apply_alias;
