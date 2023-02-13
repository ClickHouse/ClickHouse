CREATE MATERIALIZED VIEW test REFRESH AFTER 15 SECOND ENGINE = MergeTree() ORDER BY number AS SELECT * FROM system.numbers LIMIT 10;

SELECT refresh_status, last_refresh_status FROM system.view_refreshes WHERE view = 'test';
