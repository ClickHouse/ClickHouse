---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: "UUID ile \xE7al\u0131\u015Fma"
---

# UUID ile çalışmak için fonksiyonlar {#functions-for-working-with-uuid}

UUID ile çalışmak için işlevler aşağıda listelenmiştir.

## generateuuıdv4 {#uuid-function-generate}

Üretir [UUID](../../sql-reference/data-types/uuid.md) -den [sürüm 4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

**Döndürülen değer**

UUID türü değeri.

**Kullanım örneği**

Bu örnek, UUID türü sütunuyla bir tablo oluşturma ve tabloya bir değer ekleme gösterir.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUİD (x) {#touuid-x}

Dize türü değerini UUID türüne dönüştürür.

``` sql
toUUID(String)
```

**Döndürülen değer**

UUID türü değeri.

**Kullanım örneği**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUİDStringToNum {#uuidstringtonum}

Biçiminde 36 karakter içeren bir dize kabul eder `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` ve bir bayt kümesi olarak döndürür [FixedString (16)](../../sql-reference/data-types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**Döndürülen değer**

FixedString (16)

**Kullanım örnekleri**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUİDNumToString {#uuidnumtostring}

Kabul eder bir [FixedString (16)](../../sql-reference/data-types/fixedstring.md) değer ve metin biçiminde 36 karakter içeren bir dize döndürür.

``` sql
UUIDNumToString(FixedString(16))
```

**Döndürülen değer**

Dize.

**Kullanım örneği**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [dictGetUUİD](ext-dict-functions.md#ext_dict_functions-other)

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
