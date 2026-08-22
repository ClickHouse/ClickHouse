-- Tags: no-fasttest
-- encrypt function doesn't exist in the fastest build

-- { echoOn }
SET enable_analyzer = 1;

EXPLAIN QUERY TREE SELECT encrypt('aes-256-ofb', (SELECT 'qwerty'), '12345678901234567890123456789012'), encrypt('aes-256-ofb', (SELECT 'asdf'), '12345678901234567890123456789012');

SET format_display_secrets_in_show_and_select = 1;

EXPLAIN QUERY TREE SELECT encrypt('aes-256-ofb', (SELECT 'qwerty'), '12345678901234567890123456789012'), encrypt('aes-256-ofb', (SELECT 'asdf'), '12345678901234567890123456789012');
-- { echoOff }
