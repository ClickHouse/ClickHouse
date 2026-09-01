-- Do not append structured related metadata when the source description already has a cross-reference section.
SELECT name, countSubstrings(description, '**Related:**')
FROM system.documentation
WHERE type = 'Statement'
    AND name IN (
        'ALTER TABLE ... APPLY DELETED MASK',
        'CREATE NAMED COLLECTION',
        'DESCRIBE TABLE',
        'DETACH',
        'UNION')
ORDER BY name;

-- Continue appending it when no such source section exists.
SELECT countSubstrings(description, '**Related:**')
FROM system.documentation
WHERE type = 'Statement' AND name = 'ALTER TABLE ... CONSTRAINT';
