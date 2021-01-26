# Ключ и поля словаря {#kliuch-i-polia-slovaria}

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

!!! warning "Обратите внимание"
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

### Составной ключ {#sostavnoi-kliuch}

Ключом может быть кортеж (`tuple`) из полей произвольных типов. В этом случае [layout](external-dicts-dict-layout.md) должен быть `complex_key_hashed` или `complex_key_cache`.

!!! tip "Совет"
    Составной ключ может состоять из одного элемента. Это даёт возможность использовать в качестве ключа, например, строку.

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
| `type`                                               | Тип данных ClickHouse.<br/>ClickHouse пытается привести значение из словаря к заданному типу данных. Например, в случае MySQL, в таблице-источнике поле может быть `TEXT`, `VARCHAR`, `BLOB`, но загружено может быть как `String`. [Nullable](../../../sql-reference/data-types/nullable.md) не поддерживается. | Да           |
| `null_value`                                         | Значение по умолчанию для несуществующего элемента.<br/>В примере это пустая строка. Нельзя указать значение `NULL`.                                                                                                                                                                                                                                          | Да           |
| `expression`                                         | [Выражение](../../syntax.md#syntax-expressions), которое ClickHouse выполняет со значением.<br/>Выражением может быть имя столбца в удаленной SQL базе. Таким образом, вы можете использовать его для создания псевдонима удаленного столбца.<br/><br/>Значение по умолчанию: нет выражения.                                                                  | Нет          |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | Если `true`, то атрибут содержит ключ предка для текущего элемента. Смотрите [Иерархические словари](external-dicts-dict-hierarchical.md).<br/><br/>Default value: `false`.                                                                                                                                                                                   | No           |
| `is_object_id`                                       | Признак того, что запрос выполняется к документу MongoDB по `ObjectID`.<br/><br/>Значение по умолчанию: `false`.                                                                                                                                                                                                                                              | Нет          |

## Смотрите также {#smotrite-takzhe}

-   [Функции для работы с внешними словарями](../../../sql-reference/functions/ext-dict-functions.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
