SET enable_positional_arguments = 0;
SELECT k FROM (SELECT NULL, nullIf(number, 3) AS k, '1048575', (65536, -9223372036854775808), toString(number) AS a FROM system.numbers LIMIT 1048577) AS js1 ANY RIGHT JOIN (SELECT 1.000100016593933, nullIf(number, NULL) AS k, toString(number) AS b FROM system.numbers LIMIT 2, 255) AS js2 USING (k) ORDER BY 257 ASC NULLS LAST FORMAT Null;
