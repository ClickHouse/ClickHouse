# Ключ и поля словаря

Секция `<structure>` описывает ключ словаря и поля, доступные для запросов.

Общий вид структуры:

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

В структуре описываются столбцы:

- `<id>` — [ключевой столбец](external_dicts_dict_structure.md#ext_dict_structure-key).
- `<attribute>` — [столбец данных](external_dicts_dict_structure.md#ext_dict_structure-attributes). Столбцов может быть много.

## Ключ {#ext_dict_structure-key}

ClickHouse поддерживает следующие виды ключей:

- Числовой ключ. UInt64. Описывается в теге `<id>`.
- Составной ключ. Набор значений разного типа. Описывается в теге `<key>`.

Структура может содержать либо `<id>` либо `<key>`.

!!! warning "Обратите внимание"
    Ключ не надо дополнительно описывать в атрибутах.

### Числовой ключ

Тип: `UInt64`.

Пример конфигурации:

```xml
<id>
    <name>Id</name>
</id>
```

Поля конфигурации:

- `name` — имя столбца с ключами.

### Составной ключ

Ключом может быть кортеж (`tuple`) из полей произвольных типов. В этом случае [layout](external_dicts_dict_layout.md) должен быть `complex_key_hashed` или `complex_key_cache`.

!!! tip "Совет"
    Составной ключ может состоять из одного элемента. Это даёт возможность использовать в качестве ключа, например, строку.

Структура ключа задаётся в элементе `<key>`. Поля ключа задаются в том же формате, что и [атрибуты](external_dicts_dict_structure.md) словаря. Пример:

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

При запросе в функции `dictGet*` в качестве ключа передаётся кортеж. Пример: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.


## Атрибуты {#ext_dict_structure-attributes}

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

Поля конфигурации:

| Тег | Описание | Обязательный |
| ---- | ------------- | --------- |
| `name` | Имя столбца. | Да |
| `type` | Тип данных ClickHouse.<br/>ClickHouse пытается привести значение из словаря к заданному типу данных. Например, в случае MySQL, в таблице-источнике поле может быть `TEXT`, `VARCHAR`, `BLOB`, но загружено может быть как `String`. [Nullable](../../data_types/nullable.md) не поддерживается. | Да |
| `null_value` | Значение по умолчанию для несуществующего элемента.<br/>В примере это пустая строка. Нельзя указать значение `NULL`. | Да |
| `expression` | [Выражение](../syntax.md#syntax-expressions), которое ClickHouse выполняет со значением.<br/>Выражением может быть имя столбца в удаленной SQL базе. Таким образом, вы можете использовать его для создания псевдонима удаленного столбца.<br/><br/>Значение по умолчанию: нет выражения. | Нет |
| `hierarchical` | Поддержка иерархии. Отображение в идентификатор родителя.<br/><br/>Значение по умолчанию: `false`. | Нет |
| `injective` | Признак [инъективности](https://ru.wikipedia.org/wiki/Инъекция_(математика)) отображения `id -> attribute`. <br/>Если `true`, то обращения к словарям с включенной инъективностью могут быть автоматически переставлены ClickHouse за стадию `GROUP BY`, что как правило существенно сокращает их количество.<br/><br/>Значение по умолчанию: `false`. | Нет |
| `is_object_id` | Признак того, что запрос выполняется к документу MongoDB по `ObjectID`.<br/><br/>Значение по умолчанию: `false`. | Нет |

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
