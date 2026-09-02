SELECT
    DomainID,
    Domain
FROM
(
    SELECT 1 AS DomainID FROM system.one
) js1
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
