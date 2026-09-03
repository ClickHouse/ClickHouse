SELECT toTypeName(now() - now()) = 'Int32';
SELECT toTypeName(now() + 1) LIKE 'DateTime%';
SELECT toTypeName(1 + now()) LIKE 'DateTime%';
SELECT toTypeName(now() - 1) LIKE 'DateTime%';
SELECT toDateTime(1) + 1 = toDateTime(2);
SELECT 1 + toDateTime(1) = toDateTime(2);
SELECT toDateTime(1) - 1 = toDateTime(0);

SELECT toTypeName(today()) = 'Date';
SELECT today() = toDate(now());

SELECT toTypeName(yesterday()) = 'Date';

SELECT toTypeName(today() - today()) = 'Int32';
SELECT toTypeName(today() + 1) = 'Date';
SELECT toTypeName(1 + today()) = 'Date';
SELECT toTypeName(today() - 1) = 'Date';
SELECT yesterday() + 1 = today();
SELECT 1 + yesterday() = today();
SELECT today() - 1 = yesterday();
