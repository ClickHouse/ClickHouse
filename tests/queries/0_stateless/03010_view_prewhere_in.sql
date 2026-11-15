DROP TABLE IF EXISTS v;
CREATE VIEW v (`date` UInt32,`value` UInt8) AS
WITH
    data AS (SELECT '' id LIMIT 0),
    r AS (SELECT'' as id, 1::UInt8 as value)
SELECT
    now() as date,
    value AND (data.id IN (SELECT '' as d from system.one)) AS value
FROM data
         LEFT JOIN r ON data.id = r.id;

SELECT 1;
SELECT date, value FROM v;
SELECT 2;
SELECT date, value FROM v ORDER BY date;
SELECT 3;
DROP TABLE v;
