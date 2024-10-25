---
slug: /ru/sql-reference/functions/ext-dict-functions
sidebar_position: 58
sidebar_label: "Функции для работы с внешними словарями"
---

:::note Внимание
Для словарей, созданных с помощью [DDL-запросов](../../sql-reference/statements/create/dictionary.md), в параметре `dict_name` указывается полное имя словаря вместе с базой данных, например: `<database>.<dict_name>`. Если база данных не указана, используется текущая.
:::

# Функции для работы с внешними словарями {#ext_dict_functions}

Информацию о подключении и настройке внешних словарей смотрите в разделе [Внешние словари](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet, dictGetOrDefault, dictGetOrNull {#dictget}

Извлекает значение из внешнего словаря.

``` sql
dictGet('dict_name', attr_names, id_expr)
dictGetOrDefault('dict_name', attr_names, id_expr, default_value_expr)
dictGetOrNull('dict_name', attr_name, id_expr)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `attr_names` — имя столбца словаря. [Строковый литерал](../syntax.md#syntax-string-literal), или кортеж [Tuple](../../sql-reference/data-types/tuple.md) таких имен.
-   `id_expr` — значение ключа словаря. [Expression](../../sql-reference/syntax.md#syntax-expressions) возвращает пару "ключ-значение" словаря или [Tuple](../../sql-reference/functions/ext-dict-functions.md), в зависимости от конфигурации словаря.
-   `default_value_expr` — значение, возвращаемое в том случае, когда словарь не содержит строки с заданным ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions), возвращающее значение с типом данных, сконфигурированным для атрибута `attr_names`, или кортеж [Tuple](../../sql-reference/data-types/tuple.md) таких выражений.

**Возвращаемое значение**

-   Значение атрибута, соответствующее ключу `id_expr`, если ClickHouse смог привести это значение к [заданному типу данных](../../sql-reference/functions/ext-dict-functions.md#ext_dict_structure-attributes).

-   Если ключа, соответствующего `id_expr` в словаре нет, то:

    -   `dictGet` возвращает содержимое элемента `<null_value>`, указанного для атрибута в конфигурации словаря.
    -   `dictGetOrDefault` возвращает атрибут `default_value_expr`.
    -   `dictGetOrNull` возвращает `NULL` в случае, если ключ не найден в словаре.

Если значение атрибута не удалось обработать или оно не соответствует типу данных атрибута, то ClickHouse генерирует исключение.

**Пример с единственным атрибутом**

Создадим текстовый файл `ext-dict-text.csv` со следующим содержимым:

``` text
1,1
2,2
```

Первый столбец — `id`, второй столбец — `c1`.

Настройка внешнего словаря:

``` xml
<clickhouse>
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
</clickhouse>
```

Выполним запрос:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Пример с несколькими атрибутами**

Создадим текстовый файл `ext-dict-mult.csv` со следующим содержимым:

``` text
1,1,'1'
2,2,'2'
3,3,'3'
```

Первый столбец — `id`, второй столбец — `c1`, третий столбец — `c2`.

Настройка внешнего словаря:

``` xml
<clickhouse>
    <dictionary>
        <name>ext-dict-mult</name>
        <source>
            <file>
                <path>/path-to/ext-dict-mult.csv</path>
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
            <attribute>
                <name>c2</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</clickhouse>
```

Выполним запрос:

``` sql
SELECT
    dictGet('ext-dict-mult', ('c1','c2'), number + 1) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
```

``` text
┌─val─────┬─type──────────────────┐
│ (1,'1') │ Tuple(UInt8, String)  │
│ (2,'2') │ Tuple(UInt8, String)  │
│ (3,'3') │ Tuple(UInt8, String)  │
└─────────┴───────────────────────┘
```

**Пример для словаря с диапазоном ключей**

Создадим таблицу:

```sql
CREATE TABLE range_key_dictionary_source_table
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
ENGINE = TinyLog();

INSERT INTO range_key_dictionary_source_table VALUES(1, toDate('2019-05-20'), toDate('2019-05-20'), 'First', 'First');
INSERT INTO range_key_dictionary_source_table VALUES(2, toDate('2019-05-20'), toDate('2019-05-20'), 'Second', NULL);
INSERT INTO range_key_dictionary_source_table VALUES(3, toDate('2019-05-20'), toDate('2019-05-20'), 'Third', 'Third');
```

Создадим внешний словарь:

```sql
CREATE DICTIONARY range_key_dictionary
(
    key UInt64,
    start_date Date,
    end_date Date,
    value String,
    value_nullable Nullable(String)
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'range_key_dictionary_source_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);
```

Выполним запрос:

``` sql
SELECT
    (number, toDate('2019-05-20')),
    dictHas('range_key_dictionary', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value_nullable', number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', ('value', 'value_nullable'), number, toDate('2019-05-20'))
FROM system.numbers LIMIT 5 FORMAT TabSeparated;
```
Результат:

``` text
(0,'2019-05-20')        0       \N      \N      (NULL,NULL)
(1,'2019-05-20')        1       First   First   ('First','First')
(2,'2019-05-20')        1       Second  \N      ('Second',NULL)
(3,'2019-05-20')        1       Third   Third   ('Third','Third')
(4,'2019-05-20')        0       \N      \N      (NULL,NULL)
```

**Смотрите также**

-   [Внешние словари](../../sql-reference/functions/ext-dict-functions.md)

## dictHas {#dicthas}

Проверяет, присутствует ли запись с указанным ключом в словаре.

``` sql
dictHas('dict_name', id)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `id_expr` — значение ключа словаря. [Expression](../../sql-reference/syntax.md#syntax-expressions) возвращает пару "ключ-значение" словаря или [Tuple](../../sql-reference/functions/ext-dict-functions.md) в зависимости от конфигурации словаря.

**Возвращаемое значение**

-   0, если ключа нет.
-   1, если ключ есть.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

## dictGetHierarchy {#dictgethierarchy}

Создаёт массив, содержащий цепочку предков для заданного ключа в [иерархическом словаре](../dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).

**Синтаксис**

``` sql
dictGetHierarchy('dict_name', key)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `key` — значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).

**Возвращаемое значение**

-   Цепочка предков заданного ключа.

Type: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

## dictIsIn {#dictisin}

Проверяет предка ключа по всей иерархической цепочке словаря.

`dictIsIn ('dict_name', child_id_expr, ancestor_id_expr)`

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `child_id_expr` — ключ для проверки. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).
-   `ancestor_id_expr` — предполагаемый предок ключа `child_id_expr`. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).

**Возвращаемое значение**

-   0, если `child_id_expr` — не дочерний элемент `ancestor_id_expr`.
-   1, если `child_id_expr` — дочерний элемент `ancestor_id_expr` или если `child_id_expr` и есть `ancestor_id_expr`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

## dictGetChildren {#dictgetchildren}

Возвращает потомков первого уровня в виде массива индексов. Это обратное преобразование для [dictGetHierarchy](#dictgethierarchy).

**Синтаксис**

``` sql
dictGetChildren(dict_name, key)
```

**Аргументы**

-   `dict_name` — имя словаря. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
-   `key` — значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).

**Возвращаемые значения**

-   Потомки первого уровня для ключа.

Тип: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Рассмотрим иерархический словарь:

``` text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```

Потомки первого уровня:

``` sql
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetChildren('hierarchy_flat_dictionary', number)─┐
│ [1]                                                  │
│ [2,3]                                                │
│ [4]                                                  │
│ []                                                   │
└──────────────────────────────────────────────────────┘
```

## dictGetDescendant {#dictgetdescendant}

Возвращает всех потомков, как если бы функция [dictGetChildren](#dictgetchildren) была выполнена `level` раз рекурсивно.

**Синтаксис**

``` sql
dictGetDescendants(dict_name, key, level)
```

**Аргументы**

-   `dict_name` — имя словаря. [String literal](../../sql-reference/syntax.md#syntax-string-literal).
-   `key` — значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).
-   `level` — уровень иерархии. Если `level = 0`, возвращаются все потомки. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Потомки для ключа.

Тип: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Рассмотрим иерархический словарь:

``` text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```
Все потомки:

``` sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetDescendants('hierarchy_flat_dictionary', number)─┐
│ [1,2,3,4]                                               │
│ [2,3,4]                                                 │
│ [4]                                                     │
│ []                                                      │
└─────────────────────────────────────────────────────────┘
```

Потомки первого уровня:

``` sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 4;
```

``` text
┌─dictGetDescendants('hierarchy_flat_dictionary', number, 1)─┐
│ [1]                                                        │
│ [2,3]                                                      │
│ [4]                                                        │
│ []                                                         │
└────────────────────────────────────────────────────────────┘
```

## Прочие функции {#ext_dict_functions-other}

ClickHouse поддерживает специализированные функции, которые приводят значения атрибутов словаря к определённому типу данных независимо от конфигурации словаря.

Функции:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

Все эти функции можно использовать с модификатором `OrDefault`. Например, `dictGetDateOrDefault`.

Синтаксис:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `attr_name` — имя столбца словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `id_expr` — значение ключа словаря. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md) или [Tuple](../../sql-reference/functions/ext-dict-functions.md) в зависимости от конфигурации словаря.
-   `default_value_expr` — значение, возвращаемое в том случае, когда словарь не содержит строки с заданным ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions), возвращающее значение с типом данных, сконфигурированным для атрибута `attr_name`.

**Возвращаемое значение**

-   Если ClickHouse успешно обработал атрибут в соответствии с [заданным типом данных](../../sql-reference/functions/ext-dict-functions.md#ext_dict_structure-attributes), то функции возвращают значение атрибута, соответствующее ключу `id_expr`.

-   Если запрошенного `id_expr` нет в словаре, то:

    -   `dictGet[Type]` возвращает содержимое элемента `<null_value>`, указанного для атрибута в конфигурации словаря.
    -   `dictGet[Type]OrDefault` возвращает аргумент `default_value_expr`.

Если значение атрибута не удалось обработать или оно не соответствует типу данных атрибута, то ClickHouse генерирует исключение.
