SELECT 1 FROM system.one WHERE (1, 1) IN (SELECT 1 AS x, x);
SELECT 2 FROM system.one WHERE (1, 1) IN (SELECT 1 AS x, x) AND (1, 0) IN (SELECT 1 AS x, x);
SELECT 3 FROM system.one WHERE (1, 1) IN (SELECT 1 AS x, x) OR (1, 0) IN (SELECT 1 AS x, x);
SELECT 4 FROM system.one WHERE (1, 1) IN (SELECT 1 AS x, x) AND (1, 0) IN (SELECT 1 AS x, toUInt8(x - 1));
SELECT 5 FROM system.one WHERE (1, 1) IN (SELECT 1 AS x, x) OR (1, 0) IN (SELECT 1 AS x, toUInt8(x - 1));
