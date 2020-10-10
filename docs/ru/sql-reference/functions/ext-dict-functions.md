---
toc_priority: 58
toc_title: "\u0424\u0443\u043d\u043a\u0446\u0438\u0438\u0020\u0434\u043b\u044f\u0020\u0440\u0430\u0431\u043e\u0442\u044b\u0020\u0441\u0020\u0432\u043d\u0435\u0448\u043d\u0438\u043c\u0438\u0020\u0441\u043b\u043e\u0432\u0430\u0440\u044f\u043c\u0438"
---

# Функции для работы с внешними словарями {#ext_dict_functions}

Информацию о подключении и настройке внешних словарей смотрите в разделе [Внешние словари](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## dictGet {#dictget}

Извлекает значение из внешнего словаря.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Параметры**

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

**Параметры**

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

**Параметры**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `key` — значение ключа. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).

**Возвращаемое значение**

-   Цепочка предков заданного ключа.

Type: [Array(UInt64)](../../sql-reference/functions/ext-dict-functions.md).

## dictIsIn {#dictisin}

Проверяет предка ключа по всей иерархической цепочке словаря.

`dictIsIn ('dict_name', child_id_expr, ancestor_id_expr)`

**Параметры**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `child_id_expr` — ключ для проверки. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).
-   `ancestor_id_expr` — предполагаемый предок ключа `child_id_expr`. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md).

**Возвращаемое значение**

-   0, если `child_id_expr` — не дочерний элемент `ancestor_id_expr`.
-   1, если `child_id_expr` — дочерний элемент `ancestor_id_expr` или если `child_id_expr` и есть `ancestor_id_expr`.

Тип — `UInt8`.

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

**Параметры**

-   `dict_name` — имя словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `attr_name` — имя столбца словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
-   `id_expr` — значение ключа словаря. [Выражение](../syntax.md#syntax-expressions), возвращающее значение типа [UInt64](../../sql-reference/functions/ext-dict-functions.md) или [Tuple](../../sql-reference/functions/ext-dict-functions.md) в зависимости от конфигурации словаря.
-   `default_value_expr` — значение, возвращаемое в том случае, когда словарь не содержит строки с заданным ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions) возвращающее значение с типом данных, сконфигурированным для атрибута `attr_name`.

**Возвращаемое значение**

-   Если ClickHouse успешно обработал атрибут в соответствии с [заданным типом данных](../../sql-reference/functions/ext-dict-functions.md#ext_dict_structure-attributes), то функции возвращают значение атрибута, соответствующее ключу `id_expr`.

-   Если запрошенного `id_expr` нет в словаре, то:

    -   `dictGet[Type]` возвращает содержимое элемента `<null_value>`, указанного для атрибута в конфигурации словаря.
    -   `dictGet[Type]OrDefault` возвращает аргумент `default_value_expr`.

Если значение атрибута не удалось обработать или оно не соответствует типу данных атрибута, то ClickHouse генерирует исключение.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/ext_dict_functions/) <!--hide-->
