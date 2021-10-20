-- https://github.com/ClickHouse/ClickHouse/issues/30231
SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'UInt8'), toString(number),
                     CAST(number < 8, 'LowCardinality(UInt8)'), toString(number * 10),
                     CAST(number < 12, 'Nullable(UInt8)'), toString(number * 100),
                     CAST(number < 16, 'LowCardinality(Nullable(UInt8))'), toString(number * 1000),
                     toString(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
     )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'UInt8'), toString(number),
                     CAST(number < 8, 'LowCardinality(UInt8)'), toString(number * 10),
                     CAST(NULL, 'Nullable(UInt8)'), toString(number * 100),
                     CAST(NULL, 'LowCardinality(Nullable(UInt8))'), toString(number * 1000),
                     toString(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';

SELECT *
FROM (
      SELECT number,
             multiIf(
                     CAST(number < 4, 'UInt8'), toString(number),
                     CAST(number < 8, 'LowCardinality(UInt8)'), toString(number * 10)::LowCardinality(String),
                     CAST(number < 12, 'Nullable(UInt8)'), toString(number * 100)::Nullable(String),
                     CAST(number < 16, 'LowCardinality(Nullable(UInt8))'), toString(number * 1000)::LowCardinality(Nullable(String)),
                     toString(number * 10000)) as m
      FROM system.numbers
      LIMIT 20
         )
ORDER BY number
SETTINGS short_circuit_function_evaluation='enable';
