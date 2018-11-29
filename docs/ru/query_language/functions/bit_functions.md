# Битовые функции

Битовые функции работают для любой пары типов из UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.

Тип результата - целое число, битность которого равна максимальной битности аргументов. Если хотя бы один аргумент знаковый, то результат - знаковое число. Если аргумент - число с плавающей запятой - оно приводится к Int64.

## bitAnd(a, b)

## bitOr(a, b)

## bitXor(a, b)

## bitNot(a)

## bitShiftLeft(a, b)

## bitShiftRight(a, b)

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/functions/bit_functions/) <!--hide-->
