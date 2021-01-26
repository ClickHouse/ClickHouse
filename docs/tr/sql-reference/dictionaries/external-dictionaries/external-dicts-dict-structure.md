---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "S\xF6zl\xFCk anahtar\u0131 ve alanlar\u0131"
---

# Sözlük anahtarı ve alanları {#dictionary-key-and-fields}

Bu `<structure>` yan tümcesi sözlük anahtarı ve sorgular için kullanılabilir alanları açıklar.

XML açıklaması:

``` xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Nitelikler elemanlarda açıklanmıştır:

-   `<id>` — [Anahtar sütun](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [Veri sütunu](external-dicts-dict-structure.md#ext_dict_structure-attributes). Birden fazla sayıda özellik olabilir.

DDL sorgusu:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Öznitelikler sorgu gövdesinde açıklanmıştır:

-   `PRIMARY KEY` — [Anahtar sütun](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [Veri sütunu](external-dicts-dict-structure.md#ext_dict_structure-attributes). Birden fazla sayıda özellik olabilir.

## Anahtar {#ext_dict_structure-key}

ClickHouse aşağıdaki anahtar türlerini destekler:

-   Sayısal tuş. `UInt64`. Tanımlanan `<id>` etiket veya kullanma `PRIMARY KEY` kelime.
-   Kompozit anahtar. Farklı türde değerler kümesi. Etiket definedinde tanımlı `<key>` veya `PRIMARY KEY` kelime.

Bir xml yapısı şunları içerebilir `<id>` veya `<key>`. DDL sorgusu tek içermelidir `PRIMARY KEY`.

!!! warning "Uyarıcı"
    Anahtarı bir öznitelik olarak tanımlamamalısınız.

### Sayısal Tuş {#ext_dict-numeric-key}

Tür: `UInt64`.

Yapılandırma örneği:

``` xml
<id>
    <name>Id</name>
</id>
```

Yapılandırma alanları:

-   `name` – The name of the column with keys.

DDL sorgusu için:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### Kompozit Anahtar {#composite-key}

Anahtar bir olabilir `tuple` her türlü alandan. Bu [düzen](external-dicts-dict-layout.md) bu durumda olmalıdır `complex_key_hashed` veya `complex_key_cache`.

!!! tip "Uç"
    Bileşik bir anahtar tek bir elemandan oluşabilir. Bu, örneğin bir dizeyi anahtar olarak kullanmayı mümkün kılar.

Anahtar yapısı eleman ayarlanır `<key>`. Anahtar alanlar sözlük ile aynı biçimde belirtilir [öznitelik](external-dicts-dict-structure.md). Örnek:

``` xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

veya

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

Bir sorgu için `dictGet*` fonksiyon, bir tuple anahtar olarak geçirilir. Örnek: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Öznitelik {#ext_dict_structure-attributes}

Yapılandırma örneği:

``` xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

veya

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Yapılandırma alanları:

| Etiket                                               | Açıklama                                                                                                                                                                                                                                                                                                                                                     | Gerekli |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`                                               | Sütun adı.                                                                                                                                                                                                                                                                                                                                                   | Evet    |
| `type`                                               | ClickHouse veri türü.<br/>ClickHouse, sözlükten belirtilen veri türüne değer atmaya çalışır. Örneğin, MySQL için alan olabilir `TEXT`, `VARCHAR`, veya `BLOB` MySQL kaynak tablosunda, ancak şu şekilde yüklenebilir `String` Clickhouse'da.<br/>[Nullable](../../../sql-reference/data-types/nullable.md) desteklenmiyor.                                   | Evet    |
| `null_value`                                         | Varolan olmayan bir öğe için varsayılan değer.<br/>Örnekte, boş bir dizedir. Kullanamazsınız `NULL` bu alanda.                                                                                                                                                                                                                                               | Evet    |
| `expression`                                         | [İfade](../../syntax.md#syntax-expressions) bu ClickHouse değeri yürütür.<br/>İfade, uzak SQL veritabanında bir sütun adı olabilir. Bu nedenle, uzak sütun için bir diğer ad oluşturmak için kullanabilirsiniz.<br/><br/>Varsayılan değer: ifade yok.                                                                                                        | Hayır   |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | Eğer `true`, öznitelik, geçerli anahtar için bir üst anahtarın değerini içerir. Görmek [Hiyerarşik Sözlükler](external-dicts-dict-hierarchical.md).<br/><br/>Varsayılan değer: `false`.                                                                                                                                                                      | Hayır   |
| `injective`                                          | Olup olmadığını gösteren bayrak `id -> attribute` ima isge is [enjektif](https://en.wikipedia.org/wiki/Injective_function).<br/>Eğer `true`, ClickHouse sonra otomatik olarak yerleştirebilirsiniz `GROUP BY` fık .ra ile ilgili istek dictionariesleriniz Genellikle bu tür taleplerin miktarını önemli ölçüde azaltır.<br/><br/>Varsayılan değer: `false`. | Hayır   |
| `is_object_id`                                       | Bir MongoDB belgesi için sorgunun yürütülüp yürütülmediğini gösteren bayrak `ObjectID`.<br/><br/>Varsayılan değer: `false`.                                                                                                                                                                                                                                  | Hayır   |

## Ayrıca Bakınız {#see-also}

-   [Harici sözlüklerle çalışmak için işlevler](../../../sql-reference/functions/ext-dict-functions.md).

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
