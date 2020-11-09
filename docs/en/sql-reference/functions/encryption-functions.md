---
toc_priority: 
toc_title: Encryption
---

# Encryption functions {#encryption-functions}

## encrypt {#encrypt}

Supported encryption modes are listed <details markdown="1"> <summary>here</summary> 
    -   aes-128-ecb
    -   aes-192-ecb
    -   aes-256-ecb
    -   aes-128-cbc
    -   aes-192-cbc
    -   aes-256-cbc
    -   aes-128-cfb1
    -   aes-192-cfb1
    -   aes-256-cfb1
    -   aes-128-cfb8
    -   aes-192-cfb8
    -   aes-256-cfb8
    -   aes-128-cfb128
    -   aes-192-cfb128
    -   aes-256-cfb128
    -   aes-128-ofb
    -   aes-192-ofb
    -   aes-256-ofb
    -   aes-128-gcm
    -   aes-192-gcm
    -   aes-256-gcm
</details>`.

**Syntax**

```sql
encrypt(mode, plaintext, key, [iv, aad])
```

**Parameters**

-   `mode` —
-   `plaintext` —
-   `key` —
-   `iv` —
-   `add` —

## aes_encrypt_mysql {#aes_encrypt_mysql}

Function implements encryption of data with AES (Advanced Encryption Standard) algorithm. 

Supported encryption modes are listed <details markdown="1"> <summary>here</summary> 
    -   aes-128-ecb
    -   aes-192-ecb
    -   aes-256-ecb
    -   aes-128-cbc
    -   aes-192-cbc
    -   aes-256-cbc
    -   aes-128-cfb1
    -   aes-192-cfb1
    -   aes-256-cfb1
    -   aes-128-cfb8
    -   aes-192-cfb8
    -   aes-256-cfb8
    -   aes-128-cfb128
    -   aes-192-cfb128
    -   aes-256-cfb128
    -   aes-128-ofb
    -   aes-192-ofb
    -   aes-256-ofb
</details>`.

**Syntax**

```sql
aes_encrypt_mysql(mode, plaintext, key, [iv])
```

**Parameters**

-   `mode` —
-   `plaintext` —
-   `key` —
-   `iv` —

## decrypt {#decrypt}

Supported decryption modes are listed <details markdown="1"> <summary>here</summary> 
    -   aes-128-ecb
    -   aes-192-ecb
    -   aes-256-ecb
    -   aes-128-cbc
    -   aes-192-cbc
    -   aes-256-cbc
    -   aes-128-cfb1
    -   aes-192-cfb1
    -   aes-256-cfb1
    -   aes-128-cfb8
    -   aes-192-cfb8
    -   aes-256-cfb8
    -   aes-128-cfb128
    -   aes-192-cfb128
    -   aes-256-cfb128
    -   aes-128-ofb
    -   aes-192-ofb
    -   aes-256-ofb
    -   aes-128-gcm
    -   aes-192-gcm
    -   aes-256-gcm
</details>`.

**Syntax**

```sql
decrypt(mode, ciphertext, key, [iv, aad])
```

**Parameters**

-   `mode` —
-   `plaintext` —
-   `key` —
-   `iv` —
-   `add` —
## aes_decrypt_mysql {#aes_decrypt_mysql}

Function implements decryption of data with AES (Advanced Encryption Standard) algorithm. 

Supported decryption modes are listed <details markdown="1"> <summary>here</summary> 
    -   aes-128-ecb
    -   aes-192-ecb
    -   aes-256-ecb
    -   aes-128-cbc
    -   aes-192-cbc
    -   aes-256-cbc
    -   aes-128-cfb1
    -   aes-192-cfb1
    -   aes-256-cfb1
    -   aes-128-cfb8
    -   aes-192-cfb8
    -   aes-256-cfb8
    -   aes-128-cfb128
    -   aes-192-cfb128
    -   aes-256-cfb128
    -   aes-128-ofb
    -   aes-192-ofb
    -   aes-256-ofb
</details>`.

**Syntax**

```sql
aes_decrypt_mysql(mode, ciphertext, key, [iv])
```

**Parameters**

-   `mode` —
-   `plaintext` —
-   `key` —
-   `iv` —

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/encryption_functions/) <!--hide-->
