SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), (DELETE)');
SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), MATERIALIZE TTL');

SELECT '---';

SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr GROUP BY some_key), (ADD COLUMN a Int64)');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr TO VOLUME \'vol1\', expr2 + INTERVAL 2 YEAR TO VOLUME \'vol2\'), (DROP COLUMN c)');

SELECT '---';

