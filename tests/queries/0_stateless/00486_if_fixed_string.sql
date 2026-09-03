SELECT number % 2 ? 'hello' : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? 'hello' : materialize('world') FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : materialize('world') FROM system.numbers LIMIT 5;

SELECT number % 2 ? toFixedString('hello', 5) : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(toFixedString('hello', 5)) : 'world' FROM system.numbers LIMIT 5;
SELECT number % 2 ? toFixedString('hello', 5) : materialize('world') FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(toFixedString('hello', 5)) : materialize('world') FROM system.numbers LIMIT 5;

SELECT number % 2 ? 'hello' : toFixedString('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : toFixedString('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? 'hello' : materialize(toFixedString('world', 5)) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize('hello') : materialize(toFixedString('world', 5)) FROM system.numbers LIMIT 5;

SELECT number % 2 ? toFixedString('hello', 5) : toFixedString('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(toFixedString('hello', 5)) : toFixedString('world', 5) FROM system.numbers LIMIT 5;
SELECT number % 2 ? toFixedString('hello', 5) : materialize(toFixedString('world', 5)) FROM system.numbers LIMIT 5;
SELECT number % 2 ? materialize(toFixedString('hello', 5)) : materialize(toFixedString('world', 5)) FROM system.numbers LIMIT 5;
