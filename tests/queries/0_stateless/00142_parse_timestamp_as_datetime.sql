SELECT min(ts = toUInt32(toDateTime(toString(ts)))) FROM (SELECT 1000000000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
SELECT min(ts = toUInt32(toDateTime(toString(ts)))) FROM (SELECT 10000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
