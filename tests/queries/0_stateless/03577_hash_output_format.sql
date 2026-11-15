SELECT number FROM system.numbers LIMIT 1 FORMAT Hash;
SELECT number FROM system.numbers LIMIT 20 FORMAT Hash;
SELECT number AS hello, toString(number) AS world, (hello, world) AS tuple, nullIf(hello % 3, 0) AS sometimes_nulls FROM system.numbers LIMIT 20 FORMAT Hash;
