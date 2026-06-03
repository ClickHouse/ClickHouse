-- https://github.com/ClickHouse/ClickHouse/issues/64946
SELECT
    multiIf((number % toLowCardinality(toNullable(toUInt128(2)))) = (number % toNullable(2)), toInt8(1), (number % materialize(toLowCardinality(3))) = toUInt128(toNullable(0)), toInt8(materialize(materialize(2))), toInt64(toUInt128(3)))
FROM system.numbers
LIMIT 44857
FORMAT Null;
