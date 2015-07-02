SELECT number % 2 ? ['Hello', 'World'] : ['abc'] FROM system.numbers LIMIT 10;
SELECT number % 2 ? materialize(['Hello', 'World']) : ['abc'] FROM system.numbers LIMIT 10;
SELECT number % 2 ? ['Hello', 'World'] : materialize(['abc']) FROM system.numbers LIMIT 10;
SELECT number % 2 ? materialize(['Hello', 'World']) : materialize(['abc']) FROM system.numbers LIMIT 10;

SELECT number % 2 ? ['Hello', '', 'World!'] : emptyArrayString() FROM system.numbers LIMIT 10;
SELECT number % 2 ? materialize(['Hello', '', 'World!']) : emptyArrayString() FROM system.numbers LIMIT 10;

SELECT number % 2 ? [''] : ['', ''] FROM system.numbers LIMIT 10;
SELECT number % 2 ? materialize(['']) : ['', ''] FROM system.numbers LIMIT 10;
SELECT number % 2 ? [''] : materialize(['', '']) FROM system.numbers LIMIT 10;
SELECT number % 2 ? materialize(['']) : materialize(['', '']) FROM system.numbers LIMIT 10;
