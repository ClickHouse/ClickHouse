-- Tags: no-fasttest, no-old-analyzer
-- Tag no-fasttest: the encryption functions are not available in the fast test build
-- Tag no-old-analyzer: the old analyzer builds the ActionsDAG without query-tree masking, so it still leaks the key
SET explain_query_plan_default = 'legacy';
-- Secret arguments of recognized scalar functions must be hidden in the ActionsDAG dump of
-- EXPLAIN actions, just like they already are for EXPLAIN SYNTAX and EXPLAIN QUERY TREE.
-- This is general: every constant masked during analysis (encrypt/decrypt/HMAC/...) is covered.

-- { echoOn }
-- All EXPLAIN flavours must hide the key consistently.
EXPLAIN SYNTAX
SELECT encrypt('aes-128-ecb', toString(number), 'MY_SUPER_SECRET_KEY_123')
FROM numbers(3);

EXPLAIN QUERY TREE
SELECT encrypt('aes-128-ecb', toString(number), 'MY_SUPER_SECRET_KEY_123')
FROM numbers(3);

EXPLAIN actions = 1
SELECT encrypt('aes-128-ecb', toString(number), 'MY_SUPER_SECRET_KEY_123')
FROM numbers(3);

EXPLAIN actions = 1
SELECT decrypt('aes-128-ecb', toString(number), 'MY_SUPER_SECRET_KEY_123')
FROM numbers(3);

EXPLAIN actions = 1
SELECT hex(HMAC('sha256', toString(number), 'MY_SUPER_SECRET_KEY_123'))
FROM numbers(3);

-- With the setting enabled the key is shown verbatim.
SET format_display_secrets_in_show_and_select = 1;

EXPLAIN actions = 1
SELECT encrypt('aes-128-ecb', toString(number), 'MY_SUPER_SECRET_KEY_123')
FROM numbers(3);
-- { echoOff }
