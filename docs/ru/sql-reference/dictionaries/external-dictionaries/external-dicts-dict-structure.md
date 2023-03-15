---
sidebar_position: 44
sidebar_label: "Ключ и поля словаря"
---

# Ключ и поля словаря {#dictionary-key-and-fields}

Секция `<structure>` описывает ключ словаря и поля, доступные для запросов.

Описание в формате XML:

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

Атрибуты описываются элементами:

-   `<id>` — [столбец с ключом](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [столбец данных](external-dicts-dict-structure.md#ext_dict_structure-attributes). Можно задать несколько атрибутов.

Создание словаря запросом:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Атрибуты задаются в теле запроса:

-   `PRIMARY KEY` — [столбец с ключом](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [столбец данных](external-dicts-dict-structure.md#ext_dict_structure-attributes). Можно задать несколько столбцов.

## Ключ {#ext_dict_structure-key}

ClickHouse поддерживает следующие виды ключей:

-   Числовой ключ. `UInt64`. Описывается в теге `<id>` или ключевым словом `PRIMARY KEY`.
-   Составной ключ. Набор значений разного типа. Описывается в теге `<key>` или ключевым словом `PRIMARY KEY`.

Структура может содержать либо `<id>` либо `<key>`. DDL-запрос может содержать только `PRIMARY KEY`.

:::danger "Обратите внимание"
    Ключ не надо дополнительно описывать в атрибутах.

### Числовой ключ {#ext_dict-numeric-key}

Тип: `UInt64`.

Пример конфигурации:

``` xml
<id>
    <name>Id</name>
</id>
```

Поля конфигурации:

-   `name` — имя столбца с ключами.

Для DDL-запроса:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – имя столбца с ключами.

### Составной ключ {#composite-key}

Ключом может быть кортеж (`tuple`) из полей произвольных типов. В этом случае [layout](external-dicts-dict-layout.md) должен быть `complex_key_hashed` или `complex_key_cache`.

:::tip "Совет"
    Составной ключ может состоять из одного элемента. Это даёт возможность использовать в качестве ключа, например, строку.
    :::
Структура ключа задаётся в элементе `<key>`. Поля ключа задаются в том же формате, что и [атрибуты](external-dicts-dict-structure.md) словаря. Пример:

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

или

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

При запросе в функции `dictGet*` в качестве ключа передаётся кортеж. Пример: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Атрибуты {#ext_dict_structure-attributes}

Пример конфигурации:

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

или

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Поля конфигурации:

| Тег                                                  | Описание                                                                                                                                                                                                                                                                                                                                                      | Обязательный |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| `name`                                               | Имя столбца.                                                                                                                                                                                                                                                                                                                                                  | Да           |
| `type`                                               | Тип данных ClickHouse: [UInt8](../../../sql-reference/data-types/int-uint.md), [UInt16](../../../sql-reference/data-types/int-uint.md), [UInt32](../../../sql-reference/data-types/int-uint.md), [UInt64](../../../sql-reference/data-types/int-uint.md), [Int8](../../../sql-reference/data-types/int-uint.md), [Int16](../../../sql-reference/data-types/int-uint.md), [Int32](../../../sql-reference/data-types/int-uint.md), [Int64](../../../sql-reference/data-types/int-uint.md), [Float32](../../../sql-reference/data-types/float.md), [Float64](../../../sql-reference/data-types/float.md), [UUID](../../../sql-reference/data-types/uuid.md), [Decimal32](../../../sql-reference/data-types/decimal.md), [Decimal64](../../../sql-reference/data-types/decimal.md), [Decimal128](../../../sql-reference/data-types/decimal.md), [Decimal256](../../../sql-reference/data-types/decimal.md), [String](../../../sql-reference/data-types/string.md), [Array](../../../sql-reference/data-types/array.md).<br/>ClickHouse пытается привести значение из словаря к заданному типу данных. Например, в случае MySQL, в таблице-источнике поле может быть `TEXT`, `VARCHAR`, `BLOB`, но загружено может быть как `String`. <br/>[Nullable](../../../sql-reference/data-types/nullable.md) в настоящее время поддерживается для словарей [Flat](external-dicts-dict-layout.md#flat), [Hashed](external-dicts-dict-layout.md#dicts-external_dicts_dict_layout-hashed), [ComplexKeyHashed](external-dicts-dict-layout.md#complex-key-hashed), [Direct](external-dicts-dict-layout.md#direct), [ComplexKeyDirect](external-dicts-dict-layout.md#complex-key-direct), [RangeHashed](external-dicts-dict-layout.md#range-hashed), [Polygon](external-dicts-dict-polygon.md), [Cache](external-dicts-dict-layout.md#cache), [ComplexKeyCache](external-dicts-dict-layout.md#complex-key-cache), [SSDCache](external-dicts-dict-layout.md#ssd-cache), [SSDComplexKeyCache](external-dicts-dict-layout.md#complex-key-ssd-cache). Для словарей [IPTrie](external-dicts-dict-layout.md#ip-trie) `Nullable`-типы не поддерживаются. | Да           |
| `null_value`                                         | Значение по умолчанию для несуществующего элемента.<br/>В примере это пустая строка. Значение [NULL](../../syntax.md#null-literal) можно указывать только для типов `Nullable` (см. предыдущую строку с описанием типов).                                                                                                                                                                                                                                          | Да           |
| `expression`                                         | [Выражение](../../syntax.md#syntax-expressions), которое ClickHouse выполняет со значением.<br/>Выражением может быть имя столбца в удаленной SQL базе. Таким образом, вы можете использовать его для создания псевдонима удаленного столбца.<br/><br/>Значение по умолчанию: нет выражения.                                                                  | Нет          |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | Если `true`, то атрибут содержит ключ предка для текущего элемента. Смотрите [Иерархические словари](external-dicts-dict-hierarchical.md).<br/><br/>Значение по умолчанию: `false`.                                                                                                                                                                                   | Нет           |
| `is_object_id`                                       | Признак того, что запрос выполняется к документу MongoDB по `ObjectID`.<br/><br/>Значение по умолчанию: `false`.                                                                                                                                                                                                                                              | Нет          |

**Смотрите также**

-   [Функции для работы с внешними словарями](../../../sql-reference/functions/ext-dict-functions.md).
