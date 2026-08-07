SET any_join_distinct_right_table_keys = 1;

SELECT * FROM (SELECT dummy as a, (toUInt8(0), toUInt8(0)) AS tup FROM system.one) js1
JOIN (SELECT dummy as a, (toUInt8(0), toUInt8(0)) AS tup FROM system.one) js2
USING (a, tup);

SELECT * FROM (SELECT dummy as a, (toUInt8(0), toUInt8(0)) AS tup FROM system.one) js1
GLOBAL ANY FULL OUTER JOIN (SELECT dummy as a, (toUInt8(0), toUInt8(0)) AS tup FROM system.one) js2
USING (a, tup);
