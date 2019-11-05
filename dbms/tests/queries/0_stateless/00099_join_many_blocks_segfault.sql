SELECT
    1 AS DomainID,
    Domain
FROM system.one
ANY LEFT JOIN
(
    SELECT
        1 AS DomainID,
        'abc' AS Domain
    UNION ALL
    SELECT
        2 AS DomainID,
        'def' AS Domain
) js2 USING DomainID;
