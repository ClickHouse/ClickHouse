SET max_threads = 8, max_query_size = 100000, log_comment = 'session';

SELECT getSetting('max_threads'), getSetting('max_query_size'), getSetting('log_comment')
SETTINGS max_threads = 3, max_query_size = 100001, log_comment = 'query';

SELECT getSetting('max_threads'), getSetting('max_query_size'), getSetting('log_comment');

SELECT name, value, changed
FROM system.settings
WHERE name IN ('max_threads', 'max_query_size', 'log_comment')
ORDER BY name
SETTINGS max_threads = 4, max_query_size = 100002, log_comment = 'second query';

SELECT getSetting('max_threads'), getSetting('max_query_size'), getSetting('log_comment');

-- A heavily populated inherited profile still permits isolated query-local changes.
SET compatibility = '22.8';
SELECT getSetting('compatibility'), getSetting('max_threads'), getSetting('log_comment')
SETTINGS max_threads = 5, log_comment = 'compatibility query';
SELECT getSetting('compatibility'), getSetting('max_threads'), getSetting('log_comment');

SET compatibility = '';
SELECT getSetting('compatibility') = '', getSetting('max_threads'), getSetting('log_comment');
