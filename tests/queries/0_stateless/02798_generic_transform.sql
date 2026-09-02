SELECT transform((number, toString(number)), [(3, '3'), (5, '5'), (7, '7')], ['hello', 'world', 'abc!'], 'def') FROM system.numbers LIMIT 10;
SELECT transform(toNullable(toInt256(number)), [3, 5, 7], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;
SELECT transform(toUInt256(number), [3, 5, 7], ['hello', 'world', 'abc'], '') FROM system.numbers LIMIT 10;

select case 1::Nullable(Int32) when 1 then 123 else 0 end;

SELECT transform(arrayJoin(['c', 'b', 'a']), ['a', 'b'], [toDateTime64('2023-01-01', 3), toDateTime64('2023-02-02', 3)], toDateTime64('2023-03-03', 3));

SELECT transform(1, [1], [toDecimal32(1, 2)]), toDecimal32(1, 2);
select transform(1, [1], [toDecimal32(42, 2)]), toDecimal32(42, 2);
SELECT transform(1, [1], [toDecimal32(42, 2)], 0);
SELECT transform(1, [1], [toDecimal32(42, 2)], toDecimal32(0, 2));
