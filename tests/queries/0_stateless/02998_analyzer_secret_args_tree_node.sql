-- Tags: no-fasttest
-- encrypt function doesn't exist in the fastest build

-- { echoOn }
SET enable_analyzer = 1;

EXPLAIN QUERY TREE SELECT encrypt('aes-256-ofb', (SELECT 'qwerty'), '12345678901234567890123456789012'), encrypt('aes-256-ofb', (SELECT 'asdf'), '12345678901234567890123456789012');

-- The session setting alone must not reveal secrets: the `display_secrets_in_show_and_select`
-- server setting and the `displaySecretsInShowAndSelect` privilege are also required, and the
-- stateless test server has neither.
SET format_display_secrets_in_show_and_select = 1;

EXPLAIN QUERY TREE SELECT encrypt('aes-256-ofb', (SELECT 'qwerty'), '12345678901234567890123456789012'), encrypt('aes-256-ofb', (SELECT 'asdf'), '12345678901234567890123456789012');
-- { echoOff }
