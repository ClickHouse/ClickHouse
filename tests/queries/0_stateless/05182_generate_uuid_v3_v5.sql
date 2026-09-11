-- Tags: no-fasttest
-- no-fasttest: requires OpenSSL

-- Tests functions generateUUIDv3 and generateUUIDv5

SELECT 'Negative tests';
SELECT generateUUIDv3(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateUUIDv3('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'python.org'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateUUIDv5(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateUUIDv5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'python.org'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Return type';
SELECT toTypeName(generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org'));
SELECT toTypeName(generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org'));

SELECT 'Known values (RFC 4122, DNS namespace)';
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org');
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org');
SELECT generateUUIDv5(toUUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8'), 'python.org'); -- URL namespace
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), ''); -- empty name is valid
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), '');

SELECT 'Version and variant bits';
SELECT substring(hex(generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org')), 13, 1);
SELECT substring(hex(generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org')), 13, 1);
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org')), 62), 3);
SELECT bitAnd(bitShiftRight(toUInt128(generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org')), 62), 3);

SELECT 'Determinism';
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x') = generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x');
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x') = generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x');
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x') = generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'y');
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x') = generateUUIDv5(toUUID('6ba7b811-9dad-11d1-80b4-00c04fd430c8'), 'x');
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x') = generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'x');

SELECT 'Non-constant columns';
SELECT generateUUIDv3(materialize(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')), materialize('name' || toString(number))) FROM numbers(3);
SELECT generateUUIDv5(materialize(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')), materialize('name' || toString(number))) FROM numbers(3);

SELECT 'FixedString name';
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), toFixedString('python.org', 10));
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), toFixedString('python.org', 10));
