---------------------------------------------------------------------
-- Secrets can be used in encrypt/decrypt functions
SELECT ignore(decrypt('aes-256-cbc', encrypt('aes-256-cbc', 'input', secret('key32'), secret('iv')), secret('key32'), secret('iv')));
SELECT ignore(aes_decrypt_mysql('aes-256-cbc', aes_encrypt_mysql('aes-256-cbc', 'input', secret('key32'), secret('iv')), secret('key32'), secret('iv')));


-- Not expecting to connect, just make sure that secrets work with databases, tables, table functions, and dictionaries
CREATE DATABASE mysql_test_db ENGINE = MySQL(secret('mysql.dummy.host_string'), secret('mysql.dummy.database'), secret('mysql.dummy.user'), secret('mysql.dummy.password'));  -- { serverError 501 }
CREATE TABLE mysql_test_table (foo Int32 ) ENGINE = MySQL(secret('mysql.dummy.host_string'), secret('mysql.dummy.database'), secret('mysql.dummy.table'), secret('mysql.dummy.user'), secret('mysql.dummy.password'));
SELECT * FROM mysql(secret('mysql.dummy.host_string'), secret('mysql.dummy.database'), secret('mysql.dummy.table'), secret('mysql.dummy.user'), secret('mysql.dummy.password')); -- { serverError 1000 }

CREATE DICTIONARY mysql_test_dictionary (key_field1 Int32, value1 String DEFAULT 'xxx')
PRIMARY KEY key_field1
SOURCE(MYSQL(
    USER secret('mysql.dummy.user')
    PASSWORD secret('mysql.dummy.password')
    DB secret('mysql.dummy.database')
    TABLE secret('mysql.dummy.table')
    REPLICA(PRIORITY 1 HOST secret('mysql.dummy.host') PORT secret('mysql.dummy.port'))
))
LAYOUT(COMPLEX_KEY_HASHED())　LIFETIME(MIN 1 MAX 3);

-- SHOW CREATE DATABASE mysql_test_db;
SHOW CREATE TABLE mysql_test_table;
SHOW CREATE DICTIONARY mysql_test_dictionary;

------------------------------------------------------------------
-- the only way to somehow check secret value is to compare it's sha-2-256 hash.
SELECT hex(SHA256(secret('plain')));

-- it is impossible to view the secret value directly
SELECT secret('plain'); -- { clientError 48 }

SELECT secret('hierarchical.a'); -- { clientError 48 }
SELECT secret('hierarchical.b.c'); -- { clientError 48 }
SELECT secret('hierarchical.b.d.e'); -- { clientError 48 }

-- Error cases: non existing secrets
SELECT secret('nonexsting-secret'); -- { serverError 47 }
SELECT secret('hierarchical'); -- { serverError 47 }
SELECT secret('plain.foo');  -- { serverError 47 }

------------------------------------------------------------------
-- Make sure there is no illegal way to peek the secret's content
------------------------------------------------------------------

-- Common function doesn't allow secret as an input
SELECT hex(secret('plain')); -- { serverError 43 }
-- there is no way to cast a secret to string and see it that way
SELECT CAST(secret('plain') as String); -- { serverError 43 }
SELECT reinterpretAsString(secret('plain')); -- { serverError 43 }
SELECT reinterpret(secret('plain'), 'String'); -- { serverError 43 }
SELECT toString(secret('plain')); -- { serverError 43 }
SELECT toFixedString(secret('plain'), 16); -- { serverError 43 }
SELECT reinterpretAsUInt64(secret('plain'));  -- { serverError 43 }
SELECT length(secret('plain'));  -- { serverError 43 }

-- hash functions shouldn't be able to work with secret's value
SELECT SHA1(secret('plain'));   -- { serverError 43 }
SELECT SHA224(secret('plain'));   -- { serverError 43 }
SELECT MD5(secret('plain'));  -- { serverError 43 }
SELECT halfMD5(secret('plain'));  -- { serverError 43 }
SELECT intHash32(secret('plain'));  -- { serverError 43 }
SELECT intHash64(secret('plain'));  -- { serverError 43 }
SELECT URLHash(secret('plain'));  -- { serverError 43 }

SELECT farmFingerprint64(secret('plain'));   -- { serverError 43 }
SELECT farmHash64(secret('plain'));   -- { serverError 43 }
SELECT javaHash(secret('plain'));   -- { serverError 43 }
SELECT javaHashUTF16LE(secret('plain'));   -- { serverError 43 }
SELECT hiveHash(secret('plain'));   -- { serverError 43 }
SELECT metroHash64(secret('plain'));   -- { serverError 43 }
SELECT murmurHash2_32(secret('plain'));   -- { serverError 43 }
SELECT murmurHash2_64(secret('plain'));   -- { serverError 43 }
SELECT gccMurmurHash(secret('plain'));   -- { serverError 43 }

SELECT murmurHash3_32(secret('plain'));   -- { serverError 43 }
SELECT murmurHash3_64(secret('plain'));   -- { serverError 43 }
SELECT murmurHash3_128(secret('plain'));   -- { serverError 43 }

SELECT xxHash32(secret('plain'));   -- { serverError 43 }
SELECT xxHash64(secret('plain'));   -- { serverError 43 }

SELECT ngramSimHash(secret('plain'));   -- { serverError 43 }
SELECT ngramSimHashCaseInsensitive(secret('plain'));   -- { serverError 43 }
SELECT ngramSimHashUTF8(secret('plain'));   -- { serverError 43 }
SELECT ngramSimHashCaseInsensitiveUTF8(secret('plain'));   -- { serverError 43 }
SELECT cityHash64(secret('plain')); -- { serverError 43 }
SELECT sipHash64(secret('plain'));  -- { serverError 43 }
