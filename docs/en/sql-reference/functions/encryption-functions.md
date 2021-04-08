---
toc_priority: 67
toc_title: Encryption
---

# Encryption functions {#encryption-functions}

These functions  implement encryption and decryption of data with AES (Advanced Encryption Standard) algorithm.

Key length depends on encryption mode. It is 16, 24, and 32 bytes long for `-128-`, `-196-`, and `-256-` modes respectively.

Initialization vector length is always 16 bytes (bytes in excess of 16 are ignored). 

Note that these functions work slowly until ClickHouse 21.1.

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

**Arguments**

-   `mode` — Encryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — Text thats need to be encrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Encryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Required for `-gcm` modes, optinal for others. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — Additional authenticated data. It isn't encrypted, but it affects decryption. Works only in `-gcm` modes, for others would throw an exception. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Ciphertext binary string. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Create this table:

Query:

``` sql
CREATE TABLE encryption_test
(
    `comment` String,
    `secret` String
)
ENGINE = Memory;
```

Insert some data (please avoid storing the keys/ivs in the database as this undermines the whole concept of encryption), also storing 'hints' is unsafe too and used only for illustrative purposes:

Query:

``` sql
INSERT INTO encryption_test VALUES('aes-256-cfb128 no IV', encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212')),\
('aes-256-cfb128 no IV, different key', encrypt('aes-256-cfb128', 'Secret', 'keykeykeykeykeykeykeykeykeykeyke')),\
('aes-256-cfb128 with IV', encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')),\
('aes-256-cbc no IV', encrypt('aes-256-cbc', 'Secret', '12345678910121314151617181920212'));
```

Query:

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

Result:

``` text
┌─comment─────────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-cfb128 no IV                │ B4972BDC4459                     │
│ aes-256-cfb128 no IV, different key │ 2FF57C092DC9                     │
│ aes-256-cfb128 with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                   │ 1BC0629A92450D9E73A00E7D02CF4142 │
└─────────────────────────────────────┴──────────────────────────────────┘
```

Example with `-gcm`:

Query:

``` sql
INSERT INTO encryption_test VALUES('aes-256-gcm', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')), \
('aes-256-gcm with AAD', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv', 'aad'));

SELECT comment, hex(secret) FROM encryption_test WHERE comment LIKE '%gcm%';
```

Result:

``` text
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
```

## aes_encrypt_mysql {#aes_encrypt_mysql}

Compatible with mysql encryption and resulting ciphertext can be decrypted with [AES_DECRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt) function.

Will produce the same ciphertext as `encrypt` on equal inputs. But when `key` or `iv` are longer than they should normally be, `aes_encrypt_mysql` will stick to what MySQL's `aes_encrypt` does: 'fold' `key` and ignore excess bits of `iv`.

Supported encryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

``` sql
aes_encrypt_mysql('mode', 'plaintext', 'key' [, iv])
```

**Arguments**

-   `mode` — Encryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — Text that needs to be encrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Encryption key. If key is longer than required by mode, MySQL-specific key folding is performed. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Optional, only first 16 bytes are taken into account [String](../../sql-reference/data-types/string.md#string).

**Returned value**

- Ciphertext binary string. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Given equal input `encrypt` and `aes_encrypt_mysql` produce the same ciphertext:

Query:

``` sql
SELECT encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') = aes_encrypt_mysql('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') AS ciphertexts_equal;
```

Result:

```
┌─ciphertexts_equal─┐
│                 1 │
└───────────────────┘
```

But `encrypt` fails when `key` or `iv` is longer than expected:

Query:

``` sql
SELECT encrypt('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123');
```

Result:

``` text
Received exception from server (version 21.1.2):
Code: 36. DB::Exception: Received from localhost:9000. DB::Exception: Invalid key size: 33 expected 32: While processing encrypt('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123'). 
```

While `aes_encrypt_mysql` produces MySQL-compatitalbe output:

Query:

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123')) AS ciphertext;
```

Result:

```text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

Notice how supplying even longer `IV` produces the same result

Query:

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456')) AS ciphertext
```

Result:

``` text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

Which is binary equal to what MySQL produces on same inputs:

``` sql
mysql> SET  block_encryption_mode='aes-256-cfb128';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT aes_encrypt('Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456') as ciphertext;
+------------------------+
| ciphertext             |
+------------------------+
| 0x24E9E4966469         |
+------------------------+
1 row in set (0.00 sec)
```

## decrypt {#decrypt}

This function decrypts ciphertext into a plaintext using these modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Syntax**

``` sql
decrypt('mode', 'ciphertext', 'key' [, iv, aad])
```

**Arguments**

-   `mode` — Decryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — Encrypted text that needs to be decrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Decryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Required for `-gcm` modes, optinal for others. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — Additional authenticated data. Won't decrypt if this value is incorrect. Works only in `-gcm` modes, for others would throw an exception. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Decrypted String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Re-using table from [encrypt](#encrypt).

Query:

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

Result:

``` text
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
┌─comment─────────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-cfb128 no IV                │ B4972BDC4459                     │
│ aes-256-cfb128 no IV, different key │ 2FF57C092DC9                     │
│ aes-256-cfb128 with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                   │ 1BC0629A92450D9E73A00E7D02CF4142 │
└─────────────────────────────────────┴──────────────────────────────────┘
```

Now let's try to decrypt all that data.

Query:

``` sql
SELECT comment, decrypt('aes-256-cfb128', secret, '12345678910121314151617181920212') as plaintext FROM encryption_test
```

Result:

``` text
┌─comment─────────────────────────────┬─plaintext─┐
│ aes-256-cfb128 no IV                │ Secret    │
│ aes-256-cfb128 no IV, different key │ �4�
                                           �         │
│ aes-256-cfb128 with IV              │ ���6�~        │
 │aes-256-cbc no IV                   │ �2*4�h3c�4w��@
└─────────────────────────────────────┴───────────┘
```

Notice how only a portion of the data was properly decrypted, and the rest is gibberish since either `mode`, `key`, or `iv` were different upon encryption.

## aes_decrypt_mysql {#aes_decrypt_mysql}

Compatible with mysql encryption and decrypts data encrypted with [AES_ENCRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt) function.

Will produce same plaintext as `decrypt` on equal inputs. But when `key` or `iv` are longer than they should normally be, `aes_decrypt_mysql` will stick to what MySQL's `aes_decrypt` does: 'fold' `key` and ignore excess bits of `IV`.

Supported decryption modes:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Syntax**

``` sql
aes_decrypt_mysql('mode', 'ciphertext', 'key' [, iv])
```

**Arguments**

-   `mode` — Decryption mode. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — Encrypted text that needs to be decrypted. [String](../../sql-reference/data-types/string.md#string).
-   `key` — Decryption key. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — Initialization vector. Optinal. [String](../../sql-reference/data-types/string.md#string).

**Returned value**

-   Decrypted String. [String](../../sql-reference/data-types/string.md#string).

**Examples**

Let's decrypt data we've previously encrypted with MySQL:

``` sql
mysql> SET  block_encryption_mode='aes-256-cfb128';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT aes_encrypt('Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456') as ciphertext;
+------------------------+
| ciphertext             |
+------------------------+
| 0x24E9E4966469         |
+------------------------+
1 row in set (0.00 sec)
```

Query:

``` sql
SELECT aes_decrypt_mysql('aes-256-cfb128', unhex('24E9E4966469'), '123456789101213141516171819202122', 'iviviviviviviviv123456') AS plaintext
```

Result:

``` text
┌─plaintext─┐
│ Secret    │
└───────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/encryption_functions/) <!--hide-->
