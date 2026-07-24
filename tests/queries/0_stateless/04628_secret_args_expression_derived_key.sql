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
