---
description: 'Настройка ключа и атрибутов словаря'
sidebar_label: 'Атрибуты'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Атрибуты словаря'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

Клауза `structure` описывает ключ словаря и поля, доступные в запросах.

Описание XML:

```xml
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

Атрибуты описываются следующими элементами:

* `<id>` — ключевой столбец
* `<attribute>` — столбец данных: атрибутов может быть несколько.

DDL-запрос:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Атрибуты описываются в теле запроса:

* `PRIMARY KEY` — ключевой столбец
* `AttrName AttrType` — столбец данных. Атрибутов может быть несколько.

<div id="key">
  ## Ключ
</div>

ClickHouse поддерживает следующие типы ключей:

* Числовой ключ. `UInt64`. Задаётся в теге `<id>` или с помощью ключевого слова `PRIMARY KEY`.
* Составной ключ. Набор значений разных типов. Задаётся в теге `<key>` или с помощью ключевого слова `PRIMARY KEY`.

XML-структура может содержать либо `<id>`, либо `<key>`. DDL-запрос должен содержать только один `PRIMARY KEY`.

:::note
Не описывайте ключ как атрибут.
:::

<div id="numeric-key">
  ### Числовой ключ
</div>

Тип: `UInt64`.

Пример конфигурации:

```xml
<id>
    <name>Id</name>
</id>
```

Поля конфигурации:

* `name` — имя столбца с ключами.

Для DDL-запроса:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – Название столбца с ключами.

<div id="composite-key">
  ### Составной ключ
</div>

Ключом может быть `tuple` из полей любых типов. [Структура](./layouts/) в этом случае должна быть `complex_key_hashed` или `complex_key_cache`.

:::tip
Составной ключ может состоять и из одного элемента. Это позволяет, например, использовать в качестве ключа строку.
:::

Структура ключа задаётся в элементе `<key>`. Поля ключа указываются в таком же формате, как и [атрибуты](#attributes) словаря. Пример:

```xml
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

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

В запросе к функции `dictGet*` в качестве ключа передаётся кортеж. Пример: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

Если составной ключ состоит из одного атрибута, значение ключа можно передать напрямую, не оборачивая его в `tuple`. Например, и `dictGetString('dict_name', 'attr_name', 'key')`, и `dictGetString('dict_name', 'attr_name', tuple('key'))` корректны.

<div id="attributes">
  ## Атрибуты
</div>

Пример конфигурации:

```xml
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

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Поля конфигурации:

| Тег                                                | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Обязательно |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`                                             | Имя столбца.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Да          |
| `type`                                             | Тип данных ClickHouse: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br />ClickHouse пытается привести значение из словаря к указанному типу данных. Например, в MySQL поле в исходной таблице может иметь тип `TEXT`, `VARCHAR` или `BLOB`, но в ClickHouse оно может быть загружено как `String`.<br />[Nullable](../../../data-types/nullable.md) в настоящее время поддерживается для словарей [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache). В словарях [IPTrie](./layouts/ip-trie) типы `Nullable` не поддерживаются. | Да          |
| `null_value`                                       | Значение по умолчанию для несуществующего элемента.<br />В примере это пустая строка. Значение [NULL](../../../syntax.md#null) можно использовать только для типов `Nullable` (см. предыдущую строку с описанием типов).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Да          |
| `expression`                                       | [Выражение](../../../syntax.md#expressions), которое ClickHouse применяет к значению.<br />Выражение может быть именем столбца в удалённой SQL-базе данных. Таким образом, его можно использовать для создания псевдонима удалённого столбца.<br /><br />Значение по умолчанию: выражение отсутствует.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Нет         |
| <a name="hierarchical-dict-attr" /> `hierarchical` | Если `true`, атрибут содержит значение родительского ключа для текущего ключа. См. [Hierarchical Dictionaries](./layouts/hierarchical).<br /><br />Значение по умолчанию: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Нет         |
| `injective`                                        | Флаг, показывающий, является ли отображение `id -> attribute` [инъективным](https://en.wikipedia.org/wiki/Injective_function).<br />Если `true`, ClickHouse может автоматически размещать обращения к словарям для инъективных атрибутов после выражения `GROUP BY`. Обычно это значительно уменьшает количество таких обращений.<br /><br />Значение по умолчанию: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Нет         |
| `is_object_id`                                     | Флаг, показывающий, выполняется ли запрос для документа MongoDB по `ObjectID`.<br /><br />Значение по умолчанию: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |             |