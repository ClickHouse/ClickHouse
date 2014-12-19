SELECT 
    1 AS DomainID,
    Domain ANY LEFT JOIN
(
    SELECT 
        1 AS DomainID,
        'abc' AS Domain
    UNION ALL 
    SELECT 
        2 AS DomainID,
        'def' AS Domain
) USING DomainID;
