SELECT 'https://www.clickhouse.com/' AS s, REGEXP_REPLACE(s, '^https?://(?:www\.)?([^/]+)/.*$', '\1');
