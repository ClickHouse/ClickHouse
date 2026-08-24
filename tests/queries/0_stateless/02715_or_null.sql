SELECT argMaxOrNull(id, timestamp)
FROM
(
    SELECT
        CAST(NULL, 'Nullable(UInt32)') AS id,
        2 AS timestamp
);

SELECT
    argMax(id, timestamp),
    argMaxOrNull(id, timestamp)
FROM
(
    SELECT
        CAST(NULL, 'Nullable(UInt32)') AS id,
        2 AS timestamp
    UNION ALL
    SELECT
        1 AS id,
        1 AS timestamp
);

SELECT argMaxIfOrNull(id, timestamp, id IS NOT NULL)
FROM
(
    SELECT
        CAST(NULL, 'Nullable(UInt32)') AS id,
        2 AS timestamp
    UNION ALL
    SELECT
        1 AS id,
        1 AS timestamp
);
