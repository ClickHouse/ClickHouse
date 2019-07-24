SELECT 6103;

SYSTEM FLUSH LOGS;

SELECT count(query) > 0
FROM system.text_log AS natural
INNER JOIN system.query_log USING (query_id)
WHERE query = 'SELECT 6103'

