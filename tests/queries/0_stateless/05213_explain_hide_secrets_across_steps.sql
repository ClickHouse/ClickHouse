-- Tags: no-fasttest
-- no-fasttest: encrypt/decrypt/HMAC require the OpenSSL-based functions, absent in the fast test build.

-- A secret argument can be bound to a constant that is computed in another plan step: a derived
-- table, or the opposite side of a JOIN. Such an argument is an INPUT while the query is planned, and
-- plan optimizations later replace it with (or inline) the constant. The mask must travel with the
-- argument so the pretty ActionsDAG dump never prints the value. viewExplain lets us assert that no
-- fragment leaks without dumping the config-dependent plan into the reference.

SET enable_analyzer = 1;
SET format_display_secrets_in_show_and_select = 0;

-- Key from a derived table, consumed by WHERE: the filter is merged with the projection of the
-- subquery and the masked copy of the constant is deduplicated with an unmasked twin.
SELECT countIf(explain LIKE '%SEKRIT_SUBQKEY%') AS derived_table_leaks, countIf(explain LIKE '%[HIDDEN]%') > 0 AS derived_table_hidden
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT number FROM (SELECT number, 'SEKRIT_SUBQKEY' AS k FROM numbers(1)) AS s
    WHERE empty(HMAC('sha256', toString(number), s.k))));

-- Same, with the key decrypted in the subquery: the plaintext exists only as a folded constant.
SELECT countIf(explain LIKE '%SEKRIT_DECRYPTED%') AS derived_table_decrypt_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT number FROM (
        SELECT number, decrypt('aes-128-ecb', encrypt('aes-128-ecb', 'SEKRIT_DECRYPTED', '0123456789abcdef'), '0123456789abcdef') AS k
        FROM numbers(1)) AS s
    WHERE empty(HMAC('sha256', toString(number), s.k))));

-- Key from the opposite JOIN side: the ON condition is pushed down to the other side and the
-- constant is inlined in place of the unbound input.
SELECT countIf(explain LIKE '%SEKRIT_JOINKEY%') AS join_leaks, countIf(explain LIKE '%[HIDDEN]%') > 0 AS join_hidden
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT n.number FROM numbers(1) AS n
    INNER JOIN (SELECT 'SEKRIT_JOINKEY' AS k) AS s ON HMAC('sha256', toString(n.number), s.k) = ''));

SELECT countIf(explain LIKE '%SEKRIT_JOINKEY%') AS join_logical_leaks
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1, keep_logical_steps = 1', (
    SELECT n.number FROM numbers(1) AS n
    INNER JOIN (SELECT 'SEKRIT_JOINKEY' AS k) AS s ON HMAC('sha256', toString(n.number), s.k) = ''));

-- A non-constant key from the opposite JOIN side stays an input; the pretty dump must show the
-- column name, not the expression the other side computes it from.
SELECT countIf(explain LIKE '%SEKRIT_MATKEY%') AS join_column_leaks, countIf(explain LIKE '%HMAC(''sha256'', toString(number), k)%') AS join_column_shown
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT n.number FROM numbers(1) AS n
    INNER JOIN (SELECT materialize('SEKRIT_MATKEY') AS k) AS s ON HMAC('sha256', toString(n.number), s.k) = ''));

-- A column expression in a secret slot is still shown; only values are hidden.
SELECT countIf(explain LIKE '%encrypt(''aes-128-ecb'', toString(number), [HIDDEN])%') AS column_argument_shown
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT encrypt('aes-128-ecb', toString(number), 'SEKRIT_LITERALKEY') FROM numbers(1)));

-- The exact shape from the report: the HMAC key is a decrypt() of a hex ciphertext with an IV,
-- computed on the opposite JOIN side.
SELECT countIf(explain LIKE '%JOIN_SIDE_SECRET_PLAINTEXT%') AS report_join_leaks, countIf(explain LIKE '%[HIDDEN]%') > 0 AS report_join_hidden
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    SELECT n.number FROM numbers(1) AS n
    INNER JOIN (
        SELECT decrypt('aes-128-cbc', unhex(hex(encrypt('aes-128-cbc', 'JOIN_SIDE_SECRET_PLAINTEXT', '0123456789abcdef', 'abcdef9876543210'))), '0123456789abcdef', 'abcdef9876543210') AS k
    ) AS s ON HMAC('sha256', toString(n.number), s.k) = ''));

-- The report's positive control: a key bound through a WITH alias in the same scope.
SELECT countIf(explain LIKE '%WITH_ALIAS_SECRET%') AS with_alias_leaks, countIf(explain LIKE '%[HIDDEN]%') > 0 AS with_alias_hidden
FROM viewExplain('EXPLAIN PLAN', 'actions = 1, pretty = 1', (
    WITH 'WITH_ALIAS_SECRET' AS k SELECT number FROM numbers(1) WHERE empty(HMAC('sha256', toString(number), k))));
