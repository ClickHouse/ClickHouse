---
toc_priority: 67
toc_title: Encryption
---

# Encryption functions {#encryption-functions}

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
encrypt('mode', 'plaintext', 'key', [iv, aad])
```

**Parameters**

-   `mode` — Encryption mode. [String][../../sql-reference/data-types/string.md#string].
-   `plaintext` — Text thats need to be encrypted. [String][../../sql-reference/data-types/string.md#string].
-   `key` — Encryption key. Length depends on enctyption mode. [String][../../sql-reference/data-types/string.md#string].
-   `iv` — Initialization vector for block encryption modes that require it. [String][../../sql-reference/data-types/string.md#string].
-   `add` — Additional authenticated data. It doen't get encrypted, but it affects decryption. [String][../../sql-reference/data-types/string.md#string].

**Examples**

Query:

Create this table:

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

``` sql
INSERT INTO encryption_test (input)
```

## aes_encrypt_mysql {#aes_encrypt_mysql}

Function implements encryption of data with AES (Advanced Encryption Standard) algorithm. Compatible with mysql encryption and can be decrypted with `AES_DECRYPT` function.

Supported encryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

```sql
aes_encrypt_mysql('mode', 'plaintext', 'key', [iv])
```

**Parameters**

-   `mode` — Encryption mode. [String][../../sql-reference/data-types/string.md#string].
-   `plaintext` — Text that needs to be encrypted. [String][../../sql-reference/data-types/string.md#string].
-   `key` — Encryption key. Length depends on enctyption mode. [String][../../sql-reference/data-types/string.md#string].
-   `iv` — Initialization vector for block encryption modes that require it. [String][../../sql-reference/data-types/string.md#string].

## decrypt {#decrypt}

This function decrypt data using these modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Syntax**

```sql
decrypt('mode', 'ciphertext', 'key', [iv, aad])
```

**Parameters**

-   `mode` — Decryption mode. [String][../../sql-reference/data-types/string.md#string].
-   `ciphertext` — Encrypted text that needs to be decrypted. [String][../../sql-reference/data-types/string.md#string].
-   `key` — Decryption key. [String][../../sql-reference/data-types/string.md#string].
-   `iv` — Initialization vector for block encryption modes that require it. [String][../../sql-reference/data-types/string.md#string].
-   `add` — Additional authenticated data. Wont decrypt if this value is incorrect. [String][../../sql-reference/data-types/string.md#string].

## aes_decrypt_mysql {#aes_decrypt_mysql}

Function implements decryption of data with AES (Advanced Encryption Standard) algorithm. Compatible with mysql encryption and will decrypt data encrypted with `AES_ENCRYPT` function.

Supported decryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

```sql
aes_decrypt_mysql'mode', 'ciphertext', 'key', [iv])
```

**Parameters**

-   `mode` — Decryption mode. [String][../../sql-reference/data-types/string.md#string].
-   `ciphertext` — Encrypted text that needs to be decrypted. [String][../../sql-reference/data-types/string.md#string].
-   `key` — Decryption key. [String][../../sql-reference/data-types/string.md#string].
-   `iv` — Initialization vector for block encryption modes that require it. [String][../../sql-reference/data-types/string.md#string].

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/encryption_functions/) <!--hide-->
