-- Enumerate every registered SQL statement together with the complete Markdown
-- document rendered by system.documentation. The LEFT JOIN keeps an
-- undocumented registration in the output so the Python generator fails
-- closed instead of silently omitting it.
SELECT
    statements.name AS name,
    documentation.description AS description
FROM system.statements AS statements
LEFT JOIN system.documentation AS documentation
    ON documentation.type = 'Statement'
    AND documentation.name = statements.name
ORDER BY name
INTO OUTFILE 'temp-statements.jsonl' TRUNCATE FORMAT JSONEachRow
