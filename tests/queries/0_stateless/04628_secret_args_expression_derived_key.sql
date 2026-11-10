-- Tags: no-fasttest
-- no-fasttest: encrypt requires the OpenSSL-based functions, absent in the fast test build.

-- A secret argument of encrypt/decrypt is not always a bare literal: it can be built by an expression
-- (e.g. leftPad('...', 16, '*')). When secrets are not displayed, every constant used to derive the
-- key must be hidden, not just a direct literal, so no fragment of the key leaks. The mask is a
-- display flag, so it hides the value identically in the query-tree dump and the result column name.

SET enable_analyzer = 1;
SET format_display_secrets_in_show_and_select = 0;

-- Query-tree dump: the literal inside the key-deriving expression must show as [HIDDEN].
EXPLAIN QUERY TREE SELECT encrypt('aes-128-ecb', 'plaintext', leftPad('SEKRIT_DERIVEDKEY', 16, '*'));

-- Result column name (projection name): the derived-key literal must not appear in the header either.
DESCRIBE (SELECT encrypt('aes-128-ecb', 'plaintext', leftPad('SEKRIT_DERIVEDKEY', 16, '*')));

-- The ActionsDAG dump of EXPLAIN actions must hide the derived-key literal too. The pretty format
-- (the default) reads the constant value straight from the column, so it has to consult the masked
-- name instead of the raw value; the legacy format already relies on that name. viewExplain lets us
-- assert that no fragment leaks without dumping the config-dependent plan into the reference.
SELECT countIf(explain LIKE '%SEKRIT_DAGKEY%') AS pretty_dag_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (SELECT encrypt('aes-128-ecb', materialize('plaintext'), leftPad('SEKRIT_DAGKEY', 16, '*')) FROM numbers(1)));
SELECT countIf(explain LIKE '%SEKRIT_DAGKEY%') AS legacy_dag_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 0', (SELECT encrypt('aes-128-ecb', materialize('plaintext'), leftPad('SEKRIT_DAGKEY', 16, '*')) FROM numbers(1)));

-- EXPLAIN QUERY TREE with the passes disabled runs no analysis, so an ordinary secret function must be
-- masked by the dump itself; both the plaintext and the key literal must show as [HIDDEN].
EXPLAIN QUERY TREE run_passes = 0 SELECT encrypt('aes-128-ecb', 'SEKRIT_PLAINTEXT', 'SEKRIT_LITERALKEY');

-- A secret key that reaches encrypt as a column aliased in a subquery is folded into the plan as a
-- fresh constant after the query-tree masking ran, so the planner must flag that constant; the pretty
-- ActionsDAG dump must not print it.
SELECT countIf(explain LIKE '%SEKRIT_SUBQKEY16%') AS subquery_dag_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (SELECT encrypt('aes-128-ecb', toString(number), k) FROM (SELECT 'SEKRIT_SUBQKEY16' AS k, number FROM numbers(1))));

-- The folded secret key can also be an expression over several constant columns, so it is a FUNCTION
-- node carrying a constant, not a plain constant column; the plan dump must still hide it.
SELECT countIf(explain LIKE '%SEKRIT%') AS concat_dag_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (SELECT encrypt('aes-128-ecb', toString(number), concat(k1, k2)) FROM (SELECT 'SEKRIT_C' AS k1, 'ONCKEY16' AS k2, number FROM numbers(1))));
