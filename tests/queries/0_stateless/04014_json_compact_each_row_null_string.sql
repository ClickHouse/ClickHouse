-- https://github.com/ClickHouse/ClickHouse/issues/83782
SELECT c0 FROM format(JSONCompactEachRow, 'c0 String', '["A"]\n["NULL"]') ORDER BY c0;
