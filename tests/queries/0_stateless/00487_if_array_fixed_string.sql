SELECT number % 2 ? arrayMap(x -> toFixedString(x, 5), ['hello', 'world']) : arrayMap(x -> toFixedString(x, 5), ['a', 'b', 'c']) FROM system.numbers LIMIT 4;
SELECT number % 2 ? materialize(arrayMap(x -> toFixedString(x, 5), ['hello', 'world'])) : arrayMap(x -> toFixedString(x, 5), ['a', 'b', 'c']) FROM system.numbers LIMIT 4;
SELECT number % 2 ? arrayMap(x -> toFixedString(x, 5), ['hello', 'world']) : materialize(arrayMap(x -> toFixedString(x, 5), ['a', 'b', 'c'])) FROM system.numbers LIMIT 4;
SELECT number % 2 ? materialize(arrayMap(x -> toFixedString(x, 5), ['hello', 'world'])) : materialize(arrayMap(x -> toFixedString(x, 5), ['a', 'b', 'c'])) FROM system.numbers LIMIT 4;
