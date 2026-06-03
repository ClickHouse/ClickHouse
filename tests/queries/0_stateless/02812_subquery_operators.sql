SELECT singleValueOrNull(toNullable(''));
SELECT singleValueOrNull(toNullable('Hello'));
SELECT singleValueOrNull((SELECT 'Hello'));
SELECT singleValueOrNull(toNullable(123));
SELECT '' = ALL (SELECT toNullable(''));
SELECT '', ['\0'], [], singleValueOrNull(( SELECT '\0' ) ), [''];
SELECT 5 = ALL (SELECT x FROM (SELECT 1 AS x WHERE 0));
