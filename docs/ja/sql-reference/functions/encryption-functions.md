---
slug: /ja/sql-reference/functions/encryption-functions
sidebar_position: 70
sidebar_label: 暗号化
---

これらの関数は、AES（Advanced Encryption Standard）アルゴリズムを使用してデータの暗号化および復号化を実装します。

キーの長さは暗号化モードに依存します。`-128-`モードは16バイト、`-196-`モードは24バイト、`-256-`モードは32バイトの長さです。

初期化ベクトルの長さは常に16バイトです（余分なバイトは無視されます）。

これらの関数はClickHouse 21.1までは動作が遅いことに注意してください。

## encrypt

この関数は以下のモードでデータを暗号化します：

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb
- aes-128-gcm, aes-192-gcm, aes-256-gcm
- aes-128-ctr, aes-192-ctr, aes-256-ctr

**構文**

``` sql
encrypt('mode', 'plaintext', 'key' [, iv, aad])
```

**引数**

- `mode` — 暗号化モード。[String](../data-types/string.md#string)。
- `plaintext` — 暗号化されるべきテキスト。[String](../data-types/string.md#string)。
- `key` — 暗号化キー。[String](../data-types/string.md#string)。
- `iv` — 初期化ベクトル。`-gcm`モードで必要、他のモードではオプション。[String](../data-types/string.md#string)。
- `aad` — 追加の認証データ。暗号化はされませんが、復号化に影響します。`-gcm`モードでのみ動作し、他のモードでは例外をスローします。[String](../data-types/string.md#string)。

**戻り値**

- 暗号文バイナリ文字列。[String](../data-types/string.md#string)。

**例**

このテーブルを作成します：

クエリ：

``` sql
CREATE TABLE encryption_test
(
    `comment` String,
    `secret` String
)
ENGINE = Memory;
```

データを挿入します（キーやIVをデータベースに保存することは暗号化の概念を損なうため避けてください、また'ヒント'を保存するのも安全ではなく、ここでは説明のためだけに使われます）：

クエリ：

``` sql
INSERT INTO encryption_test VALUES('aes-256-ofb no IV', encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212')),\
('aes-256-ofb no IV, different key', encrypt('aes-256-ofb', 'Secret', 'keykeykeykeykeykeykeykeykeykeyke')),\
('aes-256-ofb with IV', encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')),\
('aes-256-cbc no IV', encrypt('aes-256-cbc', 'Secret', '12345678910121314151617181920212'));
```

クエリ：

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

結果：

``` text
┌─comment──────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-ofb no IV                │ B4972BDC4459                     │
│ aes-256-ofb no IV, different key │ 2FF57C092DC9                     │
│ aes-256-ofb with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                │ 1BC0629A92450D9E73A00E7D02CF4142 │
└──────────────────────────────────┴──────────────────────────────────┘
```

`-gcm`での例：

クエリ：

``` sql
INSERT INTO encryption_test VALUES('aes-256-gcm', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')), \
('aes-256-gcm with AAD', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv', 'aad'));

SELECT comment, hex(secret) FROM encryption_test WHERE comment LIKE '%gcm%';
```

結果：

``` text
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
```

## aes_encrypt_mysql

MySQLの暗号化と互換があり、結果として得られる暗号文は[AES_DECRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt)関数で復号化できます。

同じ入力で`encrypt`と同じ暗号文を生成します。ただし、`key`または`iv`が通常よりも長い場合、`aes_encrypt_mysql`はMySQLの`aes_encrypt`に従い、`key`を折りたたんで余分なビットを無視します。

サポートされている暗号化モード：

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb

**構文**

``` sql
aes_encrypt_mysql('mode', 'plaintext', 'key' [, iv])
```

**引数**

- `mode` — 暗号化モード。[String](../data-types/string.md#string)。
- `plaintext` — 暗号化されるべきテキスト。[String](../data-types/string.md#string)。
- `key` — 暗号化キー。モードが要求するよりも長い場合、MySQL固有のキーの折りたたみが行われます。[String](../data-types/string.md#string)。
- `iv` — 初期化ベクトル。オプションで、最初の16バイトのみが考慮されます。[String](../data-types/string.md#string)。

**戻り値**

- 暗号文バイナリ文字列。[String](../data-types/string.md#string)。

**例**

等しい入力で`encrypt`と`aes_encrypt_mysql`は同じ暗号文を生成します：

クエリ：

``` sql
SELECT encrypt('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') = aes_encrypt_mysql('aes-256-ofb', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') AS ciphertexts_equal;
```

結果：

```
┌─ciphertexts_equal─┐
│                 1 │
└───────────────────┘
```

しかし、`encrypt`は`key`または`iv`が予想よりも長い場合に失敗します：

クエリ：

``` sql
SELECT encrypt('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123');
```

結果：

``` text
Received exception from server (version 22.6.1):
Code: 36. DB::Exception: Received from localhost:9000. DB::Exception: Invalid key size: 33 expected 32: While processing encrypt('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123').
```

一方、`aes_encrypt_mysql`はMySQL互換の出力を生成します：

クエリ：

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123')) AS ciphertext;
```

結果：

```text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

さらに長い`IV`を提供しても同じ結果が得られることに注意してください。

クエリ：

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-ofb', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456')) AS ciphertext
```

結果：

``` text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

これは、同じ入力でMySQLが生成するものとバイナリで等しいです：

``` sql
mysql> SET  block_encryption_mode='aes-256-ofb';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT aes_encrypt('Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456') as ciphertext;
+------------------------+
| ciphertext             |
+------------------------+
| 0x24E9E4966469         |
+------------------------+
1 row in set (0.00 sec)
```

## decrypt

この関数は、以下のモードで暗号文を平文に復号します：

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-ofb, aes-192-ofb, aes-256-ofb
- aes-128-gcm, aes-192-gcm, aes-256-gcm
- aes-128-ctr, aes-192-ctr, aes-256-ctr

**構文**

``` sql
decrypt('mode', 'ciphertext', 'key' [, iv, aad])
```

**引数**

- `mode` — 復号化モード。[String](../data-types/string.md#string)。
- `ciphertext` — 復号化されるべき暗号化テキスト。[String](../data-types/string.md#string)。
- `key` — 復号化キー。[String](../data-types/string.md#string)。
- `iv` — 初期化ベクトル。`-gcm`モードで必要、他のモードではオプション。[String](../data-types/string.md#string)。
- `aad` — 追加の認証データ。この値が正しくないと復号化できません。`-gcm`モードでのみ動作し、他のモードでは例外をスローします。[String](../data-types/string.md#string)。

**戻り値**

- 復号化された文字列。[String](../data-types/string.md#string)。

**例**

[encrypt](#encrypt)のテーブルを再利用します。

クエリ：

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

結果：

``` text
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
┌─comment──────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-ofb no IV                │ B4972BDC4459                     │
│ aes-256-ofb no IV, different key │ 2FF57C092DC9                     │
│ aes-256-ofb with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                │ 1BC0629A92450D9E73A00E7D02CF4142 │
└──────────────────────────────────┴──────────────────────────────────┘
```

次に、このデータをすべて復号化してみましょう。

クエリ：

``` sql
SELECT comment, decrypt('aes-256-cfb128', secret, '12345678910121314151617181920212') as plaintext FROM encryption_test
```

結果：

``` text
┌─comment──────────────┬─plaintext──┐
│ aes-256-gcm          │ OQ�E
                             �t�7T�\���\�   │
│ aes-256-gcm with AAD │ OQ�E
                             �\��si����;�o�� │
└──────────────────────┴────────────┘
┌─comment──────────────────────────┬─plaintext─┐
│ aes-256-ofb no IV                │ Secret    │
│ aes-256-ofb no IV, different key │ �4�
                                        �         │
│ aes-256-ofb with IV              │ ���6�~        │
 │aes-256-cbc no IV                │ �2*4�h3c�4w��@
└──────────────────────────────────┴───────────┘
```

モード、キー、またはIVが暗号化時に異なっていたため、データの一部のみが適切に復号化され、残りは意味不明な文字となっています。

## tryDecrypt

`decrypt`と似ていますが、不正なキーを使用したために復号化に失敗した場合、NULLを返します。

**例**

`user_id`がユニークなユーザーIDであり、`encrypted`が暗号化された文字列フィールド、`iv`が暗号化・復号化の初期ベクトルのテーブルを作成してみましょう。ユーザーは自分のIDと暗号化されたフィールドを復号化するためのキーを知っていると仮定します：

```sql
CREATE TABLE decrypt_null (
  dt DateTime,
  user_id UInt32,
  encrypted String,
  iv String
) ENGINE = Memory;
```

データの挿入：

```sql
INSERT INTO decrypt_null VALUES
    ('2022-08-02 00:00:00', 1, encrypt('aes-256-gcm', 'value1', 'keykeykeykeykeykeykeykeykeykey01', 'iv1'), 'iv1'),
    ('2022-09-02 00:00:00', 2, encrypt('aes-256-gcm', 'value2', 'keykeykeykeykeykeykeykeykeykey02', 'iv2'), 'iv2'),
    ('2022-09-02 00:00:01', 3, encrypt('aes-256-gcm', 'value3', 'keykeykeykeykeykeykeykeykeykey03', 'iv3'), 'iv3');
```

クエリ：

```sql
SELECT
    dt,
    user_id,
    tryDecrypt('aes-256-gcm', encrypted, 'keykeykeykeykeykeykeykeykeykey02', iv) AS value
FROM decrypt_null
ORDER BY user_id ASC
```

結果：

```
┌──────────────────dt─┬─user_id─┬─value──┐
│ 2022-08-02 00:00:00 │       1 │ ᴺᵁᴸᴸ   │
│ 2022-09-02 00:00:00 │       2 │ value2 │
│ 2022-09-02 00:00:01 │       3 │ ᴺᵁᴸᴸ   │
└─────────────────────┴─────────┴────────┘
```

## aes_decrypt_mysql

MySQLの暗号化と互換性があり、データを[AES_ENCRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt)で暗号化されたデータを復号化します。

等しい入力で`decrypt`と同じ平文を生成します。ただし、`key`または`iv`が通常よりも長い場合、`aes_decrypt_mysql`はMySQLの`aes_decrypt`に従い、`key`を折りたたんで余分なビットを無視します。

サポートされている復号化モード：

- aes-128-ecb, aes-192-ecb, aes-256-ecb
- aes-128-cbc, aes-192-cbc, aes-256-cbc
- aes-128-cfb128
- aes-128-ofb, aes-192-ofb, aes-256-ofb

**構文**

``` sql
aes_decrypt_mysql('mode', 'ciphertext', 'key' [, iv])
```

**引数**

- `mode` — 復号化モード。[String](../data-types/string.md#string)。
- `ciphertext` — 復号化されるべき暗号化テキスト。[String](../data-types/string.md#string)。
- `key` — 復号化キー。[String](../data-types/string.md#string)。
- `iv` — 初期化ベクトル。オプション。[String](../data-types/string.md#string)。

**戻り値**

- 復号化された文字列。[String](../data-types/string.md#string)。

**例**

以前MySQLで暗号化したデータを復号化してみましょう：

``` sql
mysql> SET  block_encryption_mode='aes-256-ofb';
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT aes_encrypt('Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456') as ciphertext;
+------------------------+
| ciphertext             |
+------------------------+
| 0x24E9E4966469         |
+------------------------+
1 row in set (0.00 sec)
```

クエリ：

``` sql
SELECT aes_decrypt_mysql('aes-256-ofb', unhex('24E9E4966469'), '123456789101213141516171819202122', 'iviviviviviviviv123456') AS plaintext
```

結果：

``` text
┌─plaintext─┐
│ Secret    │
└───────────┘
```
