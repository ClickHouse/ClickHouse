---
toc_priority: 67
toc_title: "Функции для шифрования"
---

# Функции шифрования {#encryption-functions}

Данные функции реализуют шифрование и расшифровку данных с помощью AES (Advanced Encryption Standard) алгоритма.

Длина ключа зависит от режима шифрования. Он может быть длинной в 16, 24 и 32 байта для режимов шифрования `-128-`, `-196-` и `-256-` соответственно.

Длина инициализирующего вектора всегда 16 байт (лишние байты игнорируются).

Обратите внимание, что до версии Clickhouse 21.1 эти функции работали медленно.

## encrypt {#encrypt}

Функция поддерживает шифрование данных следующими режимами:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Синтаксис**

``` sql
encrypt('mode', 'plaintext', 'key' [, iv, aad])
```

**Аргументы**

-   `mode` — режим шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — текст, который будет зашифрован. [String](../../sql-reference/data-types/string.md#string).
-   `key` — ключ шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — инициализирующий вектор. Обязателен для `-gcm` режимов, для остальных режимов необязателен. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — дополнительные аутентифицированные данные. Не шифруются, но влияют на расшифровку. Параметр работает только с `-gcm` режимами. Для остальных вызовет исключение. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Бинарная зашифрованная строка. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Создадим такую таблицу:

Запрос:

``` sql
CREATE TABLE encryption_test
(
    `comment` String,
    `secret` String
)
ENGINE = Memory;
```

Вставим некоторые данные (замечание: не храните ключи или инициализирующие векторы в базе данных, так как это компрометирует всю концепцию шифрования), также хранение "подсказок" небезопасно и используется только для наглядности:

Запрос:

``` sql
INSERT INTO encryption_test VALUES('aes-256-cfb128 no IV', encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212')),\
('aes-256-cfb128 no IV, different key', encrypt('aes-256-cfb128', 'Secret', 'keykeykeykeykeykeykeykeykeykeyke')),\
('aes-256-cfb128 with IV', encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')),\
('aes-256-cbc no IV', encrypt('aes-256-cbc', 'Secret', '12345678910121314151617181920212'));
```

Запрос:

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

Результат:

``` text
┌─comment─────────────────────────────┬─hex(secret)──────────────────────┐
│ aes-256-cfb128 no IV                │ B4972BDC4459                     │
│ aes-256-cfb128 no IV, different key │ 2FF57C092DC9                     │
│ aes-256-cfb128 with IV              │ 5E6CB398F653                     │
│ aes-256-cbc no IV                   │ 1BC0629A92450D9E73A00E7D02CF4142 │
└─────────────────────────────────────┴──────────────────────────────────┘
```

Пример в режиме `-gcm`:

Запрос:

``` sql
INSERT INTO encryption_test VALUES('aes-256-gcm', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv')), \
('aes-256-gcm with AAD', encrypt('aes-256-gcm', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv', 'aad'));

SELECT comment, hex(secret) FROM encryption_test WHERE comment LIKE '%gcm%';
```

Результат:

``` text
┌─comment──────────────┬─hex(secret)──────────────────────────────────┐
│ aes-256-gcm          │ A8A3CCBC6426CFEEB60E4EAE03D3E94204C1B09E0254 │
│ aes-256-gcm with AAD │ A8A3CCBC6426D9A1017A0A932322F1852260A4AD6837 │
└──────────────────────┴──────────────────────────────────────────────┘
```

## aes_encrypt_mysql {#aes_encrypt_mysql}

Совместима с шифрованием myqsl, результат может быть расшифрован функцией [AES_DECRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt).

При одинаковых входящих значениях зашифрованный текст будет совпадать с результатом, возвращаемым функцией `encrypt`. Однако если `key` или `iv` длиннее, чем должны быть, `aes_encrypt_mysql` будет работать аналогично функции `aes_encrypt` в MySQL: свернет ключ и проигнорирует лишнюю часть `iv`.

Функция поддерживает шифрофание данных следующими режимами:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Синтаксис**

``` sql
aes_encrypt_mysql('mode', 'plaintext', 'key' [, iv])
```

**Аргументы**

-   `mode` — режим шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `plaintext` — текст, который будет зашифрован. [String](../../sql-reference/data-types/string.md#string).
-   `key` — ключ шифрования. Если ключ длиннее, чем требует режим шифрования, производится специфичная для MySQL свертка ключа. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — инициализирующий вектор. Необязателен, учитываются только первые 16 байтов. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Бинарная зашифрованная строка. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

При одинаковых входящих значениях результаты шифрования у функций `encrypt` и `aes_encrypt_mysql`  совпадают.

Запрос:

``` sql
SELECT encrypt('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') = aes_encrypt_mysql('aes-256-cfb128', 'Secret', '12345678910121314151617181920212', 'iviviviviviviviv') AS ciphertexts_equal;
```

Результат:

``` text
┌─ciphertexts_equal─┐
│                 1 │
└───────────────────┘
```

Функция `encrypt` генерирует исключение, если `key` или `iv` длиннее чем нужно:

Запрос:

``` sql
SELECT encrypt('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123');
```

Результат:

``` text
Received exception from server (version 21.1.2):
Code: 36. DB::Exception: Received from localhost:9000. DB::Exception: Invalid key size: 33 expected 32: While processing encrypt('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123').
```

Однако функция `aes_encrypt_mysql` в аналогичном случае возвращает результат, который может быть обработан MySQL:

Запрос:

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123')) AS ciphertext;
```

Результат:

```text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

Если передать `iv` еще длиннее, результат останется таким же:

Запрос:

``` sql
SELECT hex(aes_encrypt_mysql('aes-256-cfb128', 'Secret', '123456789101213141516171819202122', 'iviviviviviviviv123456')) AS ciphertext
```

Результат:

``` text
┌─ciphertext───┐
│ 24E9E4966469 │
└──────────────┘
```

Это совпадает с результатом, возвращаемым MySQL при таких же входящих значениях:

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

Функция расшифровывает зашифрованный текст и может работать в следующих режимах:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb
-   aes-128-gcm, aes-192-gcm, aes-256-gcm

**Синтаксис**

``` sql
decrypt('mode', 'ciphertext', 'key' [, iv, aad])
```

**Аргументы**

-   `mode` — режим шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — зашифрованный текст, который будет расшифрован. [String](../../sql-reference/data-types/string.md#string).
-   `key` — ключ шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — инициализирующий вектор. Обязателен для `-gcm` режимов, для остальных режимов опциональный. [String](../../sql-reference/data-types/string.md#string).
-   `aad` — дополнительные аутентифицированные данные. Текст не будет расшифрован, если это значение неверно. Работает только с `-gcm` режимами. Для остальных вызовет исключение. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Расшифрованная строка. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Рассмотрим таблицу из примера для функции [encrypt](#encrypt).

Запрос:

``` sql
SELECT comment, hex(secret) FROM encryption_test;
```

Результат:

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

Теперь попытаемся расшифровать эти данные:

Запрос:

``` sql
SELECT comment, decrypt('aes-256-cfb128', secret, '12345678910121314151617181920212') as plaintext FROM encryption_test;
```

Результат:

``` text
┌─comment─────────────────────────────┬─plaintext─┐
│ aes-256-cfb128 no IV                │ Secret    │
│ aes-256-cfb128 no IV, different key │ �4�
                                           �         │
│ aes-256-cfb128 with IV              │ ���6�~        │
 │aes-256-cbc no IV                   │ �2*4�h3c�4w��@
└─────────────────────────────────────┴───────────┘
```

Обратите внимание, что только часть данных была расшифрована верно. Оставшаяся часть расшифрована некорректно, так как при шифровании использовались другие значения `mode`, `key`, или `iv`.

## aes_decrypt_mysql {#aes_decrypt_mysql}

Совместима с шифрованием myqsl и может расшифровать данные, зашифрованные функцией [AES_ENCRYPT](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt).

При одинаковых входящих значениях расшифрованный текст будет совпадать с результатом, возвращаемым функцией `decrypt`. Однако если `key` или `iv` длиннее, чем должны быть, `aes_decrypt_mysql` будет работать аналогично функции `aes_decrypt` в MySQL: свернет ключ и проигнорирует лишнюю часть `iv`.

Функция поддерживает расшифровку данных в следующих режимах:

-   aes-128-ecb, aes-192-ecb, aes-256-ecb
-   aes-128-cbc, aes-192-cbc, aes-256-cbc
-   aes-128-cfb1, aes-192-cfb1, aes-256-cfb1
-   aes-128-cfb8, aes-192-cfb8, aes-256-cfb8
-   aes-128-cfb128, aes-192-cfb128, aes-256-cfb128
-   aes-128-ofb, aes-192-ofb, aes-256-ofb

**Синтаксис**

``` sql
aes_decrypt_mysql('mode', 'ciphertext', 'key' [, iv])
```

**Аргументы**

-   `mode` — режим шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `ciphertext` — зашифрованный текст, который будет расшифрован. [String](../../sql-reference/data-types/string.md#string).
-   `key` — ключ шифрования. [String](../../sql-reference/data-types/string.md#string).
-   `iv` — инициализирующий вектор. Необязателен. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Расшифрованная строка. [String](../../sql-reference/data-types/string.md#string).

**Примеры**

Расшифруем данные, которые до этого были зашифрованы в MySQL:


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

Запрос:

``` sql
SELECT aes_decrypt_mysql('aes-256-cfb128', unhex('24E9E4966469'), '123456789101213141516171819202122', 'iviviviviviviviv123456') AS plaintext;
```

Результат:

``` text
┌─plaintext─┐
│ Secret    │
└───────────┘
```
[Original article](https://clickhouse.com/docs/ru/sql-reference/functions/encryption_functions/) <!--hide-->
