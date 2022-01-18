SELECT value FROM system.one ANY LEFT JOIN (SELECT dummy, dummy AS value) js2 USING dummy GROUP BY value;

SELECT value1, value2, sum(number)
FROM (SELECT number, intHash64(number) AS value1 FROM system.numbers LIMIT 10) js1
ANY LEFT JOIN (SELECT number, intHash32(number) AS value2 FROM system.numbers LIMIT 10) js2
USING number GROUP BY value1, value2;
