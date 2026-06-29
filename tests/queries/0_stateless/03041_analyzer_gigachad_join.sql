SET enable_analyzer=1;
CREATE TABLE IF NOT EXISTS first engine = MergeTree PARTITION BY (inn, toYYYYMM(received)) ORDER BY (inn, sessionId)
AS SELECT now() AS received, '123456789' AS inn, '42' AS sessionId;

CREATE TABLE IF NOT EXISTS second engine = MergeTree PARTITION BY (inn, toYYYYMM(received)) ORDER BY (inn, sessionId)
AS SELECT now() AS received, '123456789' AS inn, '42' AS sessionId, '111' AS serial, '222' AS reg;

SELECT alias_first.inn, arrayFirst(t -> isNotNull(t), regInfo.1), arrayFirst(t -> isNotNull(t), regInfo.2)
    FROM first AS alias_first
    INNER JOIN (
        SELECT alias_second.inn, alias_second.sessionId, groupArray((serial, reg)) AS regInfo
            FROM second AS alias_second
            GROUP BY inn, sessionId
    ) AS resp ON (alias_first.inn = resp.inn) AND (alias_first.sessionId = resp.sessionId)
WHERE if('123456789' IS NOT NULL, alias_first.inn = '123456789', 1)
