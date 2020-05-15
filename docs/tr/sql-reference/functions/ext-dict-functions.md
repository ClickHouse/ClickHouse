---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "Harici S\xF6zl\xFCklerle \xE7al\u0131\u015Fma"
---

# Harici Sözlüklerle çalışmak için işlevler {#ext_dict_functions}

Dış sözlükleri bağlama ve yapılandırma hakkında bilgi için bkz. [Dış söz dictionarieslükler](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet {#dictget}

Harici bir sözlükten bir değer alır.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parametre**

-   `dict_name` — Name of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md) veya [Demet](../../sql-reference/data-types/tuple.md)- sözlük yapılandırmasına bağlı olarak değer yazın.
-   `default_value_expr` — Value returned if the dictionary doesn't contain a row with the `id_expr` anahtar. [İfade](../syntax.md#syntax-expressions) veri türü için yapılandırılmış değeri döndürme `attr_name` nitelik.

**Döndürülen değer**

-   ClickHouse özniteliği başarıyla ayrıştırırsa [öznitelik veri türü](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), fonksiyonlar karşılık gelen sözlük özniteliğinin değerini döndürür `id_expr`.

-   Anahtar yoksa, karşılık gelen `id_expr`, söz thelükte, sonra:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

Clickhouse, özniteliğin değerini ayrıştıramazsa veya değer öznitelik veri türüyle eşleşmiyorsa bir özel durum atar.

**Örnek**

Metin dosyası oluşturma `ext-dict-text.csv` aşağıdakileri içeren:

``` text
1,1
2,2
```

İlk sütun `id` ikinci sütun `c1`.

Dış sözlüğü yapılandırma:

``` xml
<yandex>
    <dictionary>
        <name>ext-dict-test</name>
        <source>
            <file>
                <path>/path-to/ext-dict-test.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</yandex>
```

Sorguyu gerçekleştir:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Ayrıca Bakınız**

-   [Dış Söz Dictionarieslükler](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)

## dictHas {#dicthas}

Bir anahtar sözlükte mevcut olup olmadığını denetler.

``` sql
dictHas('dict_name', id_expr)
```

**Parametre**

-   `dict_name` — Name of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip değeri.

**Döndürülen değer**

-   0, anahtar yoksa.
-   1, bir anahtar varsa.

Tür: `UInt8`.

## dictGetHierarchy {#dictgethierarchy}

Bir anahtarın tüm ebeveynlerini içeren bir dizi oluşturur. [hiyerarş dictionaryik sözlük](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).

**Sözdizimi**

``` sql
dictGetHierarchy('dict_name', key)
```

**Parametre**

-   `dict_name` — Name of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `key` — Key value. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip değeri.

**Döndürülen değer**

-   Anahtar için ebeveynler.

Tür: [Dizi (Uİnt64)](../../sql-reference/data-types/array.md).

## dictİsİn {#dictisin}

Sözlükteki tüm hiyerarşik zincir boyunca bir anahtarın atasını kontrol eder.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**Parametre**

-   `dict_name` — Name of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `child_id_expr` — Key to be checked. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip değeri.
-   `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` anahtar. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip değeri.

**Döndürülen değer**

-   0, eğer `child_id_expr` bir çocuk değil mi `ancestor_id_expr`.
-   1, Eğer `child_id_expr` bir çocuk `ancestor_id_expr` veya eğer `child_id_expr` is an `ancestor_id_expr`.

Tür: `UInt8`.

## Diğer Fonksiyonlar {#ext_dict_functions-other}

ClickHouse sözlük yapılandırma ne olursa olsun belirli bir veri türü için sözlük öznitelik değerlerini dönüştürmek özel işlevleri destekler.

İşlevler:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

Tüm bu işlevler `OrDefault` değişiklik. Mesela, `dictGetDateOrDefault`.

Sözdizimi:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Parametre**

-   `dict_name` — Name of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [String lit literal](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [İfade](../syntax.md#syntax-expressions) dönen bir [Uİnt64](../../sql-reference/data-types/int-uint.md)- tip değeri.
-   `default_value_expr` — Value which is returned if the dictionary doesn't contain a row with the `id_expr` anahtar. [İfade](../syntax.md#syntax-expressions) veri türü için yapılandırılmış bir değer döndürme `attr_name` nitelik.

**Döndürülen değer**

-   ClickHouse özniteliği başarıyla ayrıştırırsa [öznitelik veri türü](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), fonksiyonlar karşılık gelen sözlük özniteliğinin değerini döndürür `id_expr`.

-   İsten nomiyorsa `id_expr` söz thelükte o zaman:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

Clickhouse, özniteliğin değerini ayrıştıramazsa veya değer öznitelik veri türüyle eşleşmiyorsa bir özel durum atar.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
