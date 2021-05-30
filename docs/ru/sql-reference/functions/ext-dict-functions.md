---
toc_priority: 58
toc_title: "Функции для работы с внешними словарями"
---

# Функции для работы с внешними словарями {#ext_dict_functions}

Информацию о подключении и настройке внешних словарей смотрите в разделе [Внешние словари](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet {#dictget}

Извлекает значение из внешнего словаря.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `attr_name` — имя столбца словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `id_expr` — значение ключа словаря. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md) или [Tuple](../../sql-reference/functions/ext-dict-functions.md) в зависимости от конфигурации словаря.
-   `default_value_expr` — значение, возвращаемое в том случае, когда словарь не содержит строки с заданным ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions) возвращающее значение с типом данных, сконфигурированным для атрибута `attr_name`.

**Возвращаемое значение**

-   Значение атрибута, соответствующее ключу `id_expr`, если ClickHouse смог привести это значение к [заданному типу данных](../../sql-reference/functions/ext-dict-functions.md#ext_dict_structure-attributes).

-   Если ключа, соответствующего `id_expr` в словаре нет, то:

    -   `dictGet` возвращает содержимое элемента `<null_value>`, указанного для атрибута в конфигурации словаря.
    -   `dictGetOrDefault` возвращает атрибут `default_value_expr`.

Если значение атрибута не удалось обработать или оно не соответствует типу данных атрибута, то ClickHouse генерирует исключение.

**Пример**

Создадим текстовый файл `ext-dict-text.csv` со следующим содержимым:

``` text
1,1
2,2
```

Первый столбец — `id`, второй столбец — `c1`.

Настройка внешнего словаря:

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

Выполним запрос:

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

**Смотрите также**

-   [Внешние словари](../../sql-reference/functions/ext-dict-functions.md)

## dictHas {#dicthas}

Проверяет, присутствует ли запись с указанным ключом в словаре.

``` sql
dictHas('dict_name', id)
```

**Аргументы**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `id_expr` — значение ключа словаря. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md) или [Tuple](../../sql-reference/functions/ext-dict-functions.md) в зависимости от конфигурации словаря.

**Возвращаемое значение**

-   0, если ключа нет.
-   1, если ключ есть.

Тип — `UInt8`.

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

Type: [Array(UInt64)](../../sql-reference/functions/ext-dict-functions.md).

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

Тип — `UInt8`.

## dictGetChildren {#dictgetchildren}

Возвращает потомков первого уровня в виде массива индексов. Это обратное преобразование для [dictGetHierarchy](#dictgethierarchy).

**Синтаксис** 

``` sql
dictGetChildren(dict_name, key)
```

**Аргументы** 

-   `dict_name` — Имя словаря. [String literal](../../sql-reference/syntax.md#syntax-string-literal). 
-   `key` — Значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md). 

**Возвращаемые значения**

-   Потомки первого уровня для ключа.

Тип: [Array(UInt64)](../../sql-reference/data-types/array.md).

## dictGetDescendant {#dictgetdescendant}

Возвращает всех потомков, как если бы функция [dictGetChildren](#dictgetchildren) была выполнена `level` раз рекурсивно.  

**Синтаксис**

``` sql
dictGetDescendants(dict_name, key, level)
```

**Аргументы** 

-   `dict_name` — Имя словаря. [String literal](../../sql-reference/syntax.md#syntax-string-literal). 
-   `key` — Значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).
-   `level` — уровень иерархии. Если `level = 0` возвращаются все потомки до конца. [UInt8](../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения**

-   Потомки для ключа.

Тип: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Пример**

Исходная таблица:

``` sql
CREATE TABLE hierarchy_source_table (id UInt64, parent_id UInt64) ENGINE = TinyLog;
INSERT INTO hierarchy_source_table VALUES (1, 0), (2, 1), (3, 1), (4, 2);
CREATE DICTIONARY hierarchy_flat_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hierarchy_source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 1 MAX 1000);
```
Запрос:

``` sql
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 4;
```
Результат:

``` text
┌─dictGetChildren('hierarchy_flat_dictionary', number)─┐
│ [1]                                                  │
│ [2,3]                                                │
│ [4]                                                  │
│ []                                                   │
└──────────────────────────────────────────────────────┘
┌─dictGetDescendants('hierarchy_flat_dictionary', number)─┐
│ [1,2,3,4]                                               │
│ [2,3,4]                                                 │
│ [4]                                                     │
│ []                                                      │
└─────────────────────────────────────────────────────────┘
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
