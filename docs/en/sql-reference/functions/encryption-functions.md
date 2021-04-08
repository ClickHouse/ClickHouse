---
toc_priority: 67
toc_title: Encryption
---

# Encryption functions {#encryption-functions}

These functions  implement encryption and decryption of data with AES (Advanced Encryption Standard) algorithm.

Key length depends on encryption mode. It is 16, 24, and 32 bytes long for `-128-`, `-196-`, and `-256-` modes respectively.

Initialization vector length is always 16 bytes (bytes in excess of 16 are ignored). 

Note that these functions work slowly.

## encrypt {#encrypt}

This function encrypts data using these modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Syntax**

``` sql
encrypt('mode', 'plaintext', 'key' [, iv, aad])
```

**Parameters**

-   `mode` — Encryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — Text thats need to be encrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Encryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Required for `-gcm` modes, optinal for others. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — Additional authenticated data. It isn't encrypted, but it affects decryption. Works only in `-gcm` modes, for others would throw an exception. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Ciphered String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Create this table:

Query:

``` sql
CREATE TABLE encryption_test
(
    input String,
    key String DEFAULT unhex('fb9958e2e897ef3fdb49067b51a24af645b3626eed2f9ea1dc7fd4dd71b7e38f9a68db2a3184f952382c783785f9d77bf923577108a88adaacae5c141b1576b0'),
    iv String DEFAULT unhex('8CA3554377DFF8A369BC50A89780DD85'),
    key32 String DEFAULT substring(key, 1, 32),
    key24 String DEFAULT substring(key, 1, 24),
    key16 String DEFAULT substring(key, 1, 16)
) Engine = Memory;
```

Insert this data:

Query:

``` sql
INSERT INTO encryption_test (input) VALUES (''), ('text'), ('What Is ClickHouse?');
```

Example without `iv`:

Query:

``` sql
SELECT 'aes-128-ecb' AS mode, hex(encrypt(mode, input, key16)) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─hex(encrypt('aes-128-ecb', input, key16))────────────────────────┐
│ aes-128-ecb │ 4603E6862B0D94BBEC68E0B0DF51D60F                                 │
│ aes-128-ecb │ 3004851B86D3F3950672DE7085D27C03                                 │
│ aes-128-ecb │ E807F8C8D40A11F65076361AFC7D8B68D8658C5FAA6457985CAA380F16B3F7E4 │
└─────────────┴──────────────────────────────────────────────────────────────────┘
```

Example with `iv`:

Query:

``` sql
SELECT 'aes-256-ctr' AS mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─hex(encrypt('aes-256-ctr', input, key32, iv))─┐
│ aes-256-ctr │                                               │
│ aes-256-ctr │ 7FB039F7                                      │
│ aes-256-ctr │ 5CBD20F7ABD3AC41FCAA1A5C0E119E2B325949        │
└─────────────┴───────────────────────────────────────────────┘
```

Example with `-gcm`:

Query:

``` sql
SELECT 'aes-256-gcm' AS mode, hex(encrypt(mode, input, key32, iv)) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─hex(encrypt('aes-256-gcm', input, key32, iv))──────────────────────────┐
│ aes-256-gcm │ E99DBEBC01F021758352D7FBD9039EFA                                       │
│ aes-256-gcm │ 8742CE3A7B0595B281C712600D274CA881F47414                               │
│ aes-256-gcm │ A44FD73ACEB1A64BDE2D03808A2576EDBB60764CC6982DB9AF2C33C893D91B00C60DC5 │
└─────────────┴────────────────────────────────────────────────────────────────────────┘
```

Example with `-gcm` mode and with `aad`:

Query:

``` sql
SELECT 'aes-192-gcm' AS mode, hex(encrypt(mode, input, key24, iv, 'AAD')) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─hex(encrypt('aes-192-gcm', input, key24, iv, 'AAD'))───────────────────┐
│ aes-192-gcm │ 04C13E4B1D62481ED22B3644595CB5DB                                       │
│ aes-192-gcm │ 9A6CF0FD2B329B04EAD18301818F016DF8F77447                               │
│ aes-192-gcm │ B961E9FD9B940EBAD7ADDA75C9F198A40797A5EA1722D542890CC976E21113BBB8A7AA │
└─────────────┴────────────────────────────────────────────────────────────────────────┘
```

## aes_encrypt_mysql {#aes_encrypt_mysql}

Compatible with mysql encryption and can be decrypted with [AES_DECRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt) function.

Supported encryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

```sql
aes_encrypt_mysql('mode', 'plaintext', 'key' [, iv])
```

**Parameters**

-   `mode` — Encryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — Text that needs to be encrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Encryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Optinal. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Ciphered String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Create this table:

Query:

``` sql
CREATE TABLE encryption_test
(
    input String,
    key String DEFAULT unhex('fb9958e2e897ef3fdb49067b51a24af645b3626eed2f9ea1dc7fd4dd71b7e38f9a68db2a3184f952382c783785f9d77bf923577108a88adaacae5c141b1576b0'),
    iv String DEFAULT unhex('8CA3554377DFF8A369BC50A89780DD85'),
    key32 String DEFAULT substring(key, 1, 32),
    key24 String DEFAULT substring(key, 1, 24),
    key16 String DEFAULT substring(key, 1, 16)
) Engine = Memory;
```

Insert this data:

Query:

``` sql
INSERT INTO encryption_test (input) VALUES (''), ('text'), ('What Is ClickHouse?');
```

Example without `iv`:

Query:

``` sql
SELECT 'aes-128-cbc' AS mode, hex(aes_encrypt_mysql(mode, input, key32)) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─hex(aes_encrypt_mysql('aes-128-cbc', input, key32))──────────────┐
│ aes-128-cbc │ FEA8CFDE6EE2C6E7A2CC6ADDC9F62C83                                 │
│ aes-128-cbc │ 78B16CD4BE107660156124C5FEE6454A                                 │
│ aes-128-cbc │ 67C0B119D96F18E2823968D42871B3D179221B1E7EE642D628341C2B29BA2E18 │
└─────────────┴──────────────────────────────────────────────────────────────────┘
```

Example with `iv`:

Query:

``` sql
SELECT 'aes-256-cfb128' AS mode, hex(aes_encrypt_mysql(mode, input, key32, iv)) FROM encryption_test;
```

Result:

``` text
┌─mode───────────┬─hex(aes_encrypt_mysql('aes-256-cfb128', input, key32, iv))─┐
│ aes-256-cfb128 │                                                            │
│ aes-256-cfb128 │ 7FB039F7                                                   │
│ aes-256-cfb128 │ 5CBD20F7ABD3AC41FCAA1A5C0E119E2BB5174F                     │
└────────────────┴────────────────────────────────────────────────────────────┘
```

## decrypt {#decrypt}

This function decrypts data using these modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Syntax**

```sql
decrypt('mode', 'ciphertext', 'key' [, iv, aad])
```

**Parameters**

-   `mode` — Decryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — Encrypted text that needs to be decrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Decryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Required for `-gcm` modes, optinal for others. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for others would throw an exception. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Decrypted String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Create this table:

Query:

``` sql
CREATE TABLE encryption_test
(
    input String,
    key String DEFAULT unhex('fb9958e2e897ef3fdb49067b51a24af645b3626eed2f9ea1dc7fd4dd71b7e38f9a68db2a3184f952382c783785f9d77bf923577108a88adaacae5c141b1576b0'),
    iv String DEFAULT unhex('8CA3554377DFF8A369BC50A89780DD85'),
    key32 String DEFAULT substring(key, 1, 32),
    key24 String DEFAULT substring(key, 1, 24),
    key16 String DEFAULT substring(key, 1, 16)
) Engine = Memory;
```

Insert this data:

Query:

``` sql
INSERT INTO encryption_test (input) VALUES (''), ('text'), ('What Is ClickHouse?');
```

Query:

``` sql

SELECT 'aes-128-ecb' AS mode, decrypt(mode, encrypt(mode, input, key16), key16) FROM encryption_test;
```

Result:

```text
┌─mode────────┬─decrypt('aes-128-ecb', encrypt('aes-128-ecb', input, key16), key16)─┐
│ aes-128-ecb │                                                                     │
│ aes-128-ecb │ text                                                                │
│ aes-128-ecb │ What Is ClickHouse?                                                 │
└─────────────┴─────────────────────────────────────────────────────────────────────┘
```

## aes_decrypt_mysql {#aes_decrypt_mysql}

Compatible with mysql encryption and decrypts data encrypted with [AES_ENCRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt) function.

Supported decryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

```sql
aes_decrypt_mysql('mode', 'ciphertext', 'key' [, iv])
```

**Parameters**

-   `mode` — Decryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — Encrypted text that needs to be decrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Decryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Optinal. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Decrypted String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Create this table:

Query:

``` sql
CREATE TABLE encryption_test
(
    input String,
    key String DEFAULT unhex('fb9958e2e897ef3fdb49067b51a24af645b3626eed2f9ea1dc7fd4dd71b7e38f9a68db2a3184f952382c783785f9d77bf923577108a88adaacae5c141b1576b0'),
    iv String DEFAULT unhex('8CA3554377DFF8A369BC50A89780DD85'),
    key32 String DEFAULT substring(key, 1, 32),
    key24 String DEFAULT substring(key, 1, 24),
    key16 String DEFAULT substring(key, 1, 16)
) Engine = Memory;
```

Insert this data:

Query:

``` sql
INSERT INTO encryption_test (input) VALUES (''), ('text'), ('What Is ClickHouse?');
```

Query:

``` sql
SELECT 'aes-128-cbc' AS mode, aes_decrypt_mysql(mode, aes_encrypt_mysql(mode, input, key), key) FROM encryption_test;
```

Result:

``` text
┌─mode────────┬─aes_decrypt_mysql('aes-128-cbc', aes_encrypt_mysql('aes-128-cbc', input, key), key)─┐
│ aes-128-cbc │                                                                                     │
│ aes-128-cbc │ text                                                                                │
│ aes-128-cbc │ What Is ClickHouse?                                                                 │
└─────────────┴─────────────────────────────────────────────────────────────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/encryption_functions/) <!--hide-->
