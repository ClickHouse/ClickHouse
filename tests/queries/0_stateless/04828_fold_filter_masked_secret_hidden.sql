-- Tags: no-fasttest
-- no-fasttest: `encrypt` needs OpenSSL

SET enable_analyzer = 1;

-- Constant filter folding through `materialize` rebuilds the filter output as a fresh COLUMN
-- node. When a folded constant is a masked secret (here: the `encrypt` key reused in the WHERE
-- through an alias), the rebuilt node must keep `is_masked_secret`, so plan dumps render it as
-- [HIDDEN]. The `cond` alias keeps the filter column name free of the [HIDDEN...] placeholder,
-- so only the flag hides the value.
SELECT 'masked secret filter stays hidden', countIf(explain LIKE '%Filter column: [HIDDEN]%')
FROM (EXPLAIN PLAN actions = 1
    WITH '1234567890123456' AS key, materialize(key) = '1234567890123456' AS cond
    SELECT encrypt('aes-128-ecb', 'v', key) FROM numbers(1) WHERE cond);
SELECT 'folded value not printed', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1
    WITH '1234567890123456' AS key, materialize(key) = '1234567890123456' AS cond
    SELECT encrypt('aes-128-ecb', 'v', key) FROM numbers(1) WHERE cond);

-- the fold itself still happens and the filter passes
SELECT count() FROM (
    WITH '1234567890123456' AS key, materialize(key) = '1234567890123456' AS cond
    SELECT encrypt('aes-128-ecb', 'v', key) FROM numbers(1) WHERE cond);
