-- https://github.com/ClickHouse/ClickHouse/issues/97454
SET enable_analyzer = 1;
CREATE VIEW v0 AS (SELECT 1 c0);
CREATE DICTIONARY d0 (c0 Nullable(String)) PRIMARY KEY (c0) SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'v0')) LAYOUT(IP_TRIE()) LIFETIME(1);
SELECT c0.null FROM d0; -- { serverError BAD_ARGUMENTS }
