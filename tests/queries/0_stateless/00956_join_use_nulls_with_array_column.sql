SET join_use_nulls = 1;
SELECT number FROM (SELECT number from system.numbers LIMIT 10) as js1 SEMI LEFT JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) LIMIT 1;
SELECT number FROM (SELECT number from system.numbers LIMIT 10) as js1 ANY LEFT  JOIN (SELECT number, ['test'] FROM system.numbers LIMIT 1) js2 USING (number) ORDER BY number LIMIT 1;
