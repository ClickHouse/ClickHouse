-- Make sure that any kind of `VIEW` can be created with a `COMMENT` clause
-- and value of that clause is visible as `comment` column of `system.tables` table.

CREATE VIEW view_comment_test AS (SELECT 1) COMMENT 'simple view';
CREATE MATERIALIZED VIEW materialized_view_comment_test TO test1 (a UInt64) AS (SELECT 1) COMMENT 'materialized view';

SET allow_experimental_live_view=1;
CREATE LIVE VIEW live_view_comment_test AS (SELECT 1) COMMENT 'live view';

SYSTEM FLUSH LOGS;

SELECT name, engine, comment FROM system.tables WHERE database == currentDatabase() ORDER BY name;
