-- Forward compatibility: accept COMMENT before AS SELECT (syntax produced by 26.2+).
SELECT formatQuery('CREATE VIEW v COMMENT \'test\' AS SELECT 1');
SELECT formatQuery('CREATE MATERIALIZED VIEW v ENGINE = MergeTree ORDER BY c COMMENT \'test\' AS SELECT 1 AS c');
