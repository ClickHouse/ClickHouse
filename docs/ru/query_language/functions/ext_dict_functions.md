# Функции для работы с внешними словарями {#ext_dict_functions}

Для получения информации о подключении и настройке, читайте раздел про [внешние словари](../dicts/external_dicts.md).

## dictGet

Получение значения из внешнего словаря.

```
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Параметры**

- `dict_name` — Название словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `attr_name` — Название колонки словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `id_expr` — Значение ключа. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md) или [Tuple](../../data_types/tuple.md) в зависимости от конфигурации словаря.
- `default_value_expr` — Значение которое возвращается, если словарь не содержит колонку с ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions) возвращает значение такого же типа, что и у атрибута `attr_name`.

**Возвращаемое значение**

- Если ClickHouse успешно обрабатывает атрибут в соотвествии с указаным [типом данных](../dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes), то функция возвращает значение для заданного ключа `id_expr`.
- Если запрашиваемого `id_expr` не оказалось в словаре:

    - `dictGet` возвратит содержимое элемента `<null_value>` определенного в настройках словаря.
    - `dictGetOrDefault` вернет значение переданного `default_value_expr` параметра.

ClickHouse бросает исключение, если не может обработать значение атрибута или значение несопоставимо с типом атрибута.

**Пример использования**

Создайте файл `ext-dict-text.csv` со следующим содержимым:

```text
1,1
2,2
```

Первая колонка - это `id`, вторая - `c1`

Конфигурация внешнего словаря:

```xml
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
                <Тип>UInt32</Тип>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</yandex>
```

Выполните запрос:

```sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toТипName(val) AS Тип
FROM system.numbers
LIMIT 3
```
```text
┌─val─┬─Тип───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**Смотрите также**

- [Внешние словари](../dicts/external_dicts.md)


## dictHas

Проверяет наличие строки с заданным ключом в словаре.

```
dictHas('dict_name', id_expr)
```

**Параметры**

- `dict_name` — Название словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `id_expr` — Значение ключа. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md).

**Возвращаемое значение**

- 0, если ключ не был обнаружен
- 1, если ключ присутствует в словаре

Тип: `UInt8`.

## dictGetHierarchy

Для иерархических словарей, возвращает массив ключей, содержащий ключ `id_expr` и все ключи родительских элементов по цепочке.

```
dictGetHierarchy('dict_name', id_expr)
```

**Параметры**

- `dict_name` — Название словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `id_expr` — Значение ключа. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md).

**Возвращаемое значение**

Иерархию ключей словаря.

Тип: [Array(UInt64)](../../data_types/array.md).

## dictIsIn

Осуществляет проверку - является ли ключ родительским в иерархии словаря.

`dictIsIn ('dict_name', child_id_expr, ancestor_id_expr)`

**Параметры**

- `dict_name` — Название словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `child_id_expr` — Ключ который должен быть проверен. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md).
- `ancestor_id_expr` — Родительский ключ для ключа `child_id_expr`. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md).

**Возвращаемое значение**

- 0, если `child_id_expr` не является потомком для `ancestor_id_expr`.
- 1, если `child_id_expr` является потомком для `ancestor_id_expr` или если `child_id_expr` равен `ancestor_id_expr`.

Тип: `UInt8`.

## Другие функции {#ext_dict_functions-other}

ClickHouse поддерживает специализированные функции для конвертации значений атрибутов словаря к определенному типу, независимо от настроек словаря.

Функции:

- `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
- `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
- `dictGetFloat32`, `dictGetFloat64`
- `dictGetDate`
- `dictGetDateTime`
- `dictGetUUID`
- `dictGetString`

Все эти функции имеют так же `OrDefault` версию. Например, `dictGetDateOrDefault`.

Синтаксис:

```
dictGet[Тип]('dict_name', 'attr_name', id_expr)
dictGet[Тип]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**Параметры**

- `dict_name` — Название словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `attr_name` — Название колонки словаря. [Строковый литерал](../syntax.md#syntax-string-literal).
- `id_expr` — Значение ключа. [Выражение](../syntax.md#syntax-expressions) возвращает значение типа [UInt64](../../data_types/int_uint.md).
- `default_value_expr` — Значение которое возвращается, если словарь не содержит строку с ключом `id_expr`. [Выражение](../syntax.md#syntax-expressions) возвращает значение с таким же типом, что и тип атрибута `attr_name`.

**Возвращаемое значение**

- Если ClickHouse успешно обрабатывает атрибут в соотвествии с указаным [типом данных](../dicts/external_dicts_dict_structure.md#ext_dict_structure-attributes),то функция возвращает значение для заданного ключа `id_expr`.
- Если запращиваемого `id_expr` не оказалось в словаре:

    - `dictGet[Тип]` возвратит содержимое элемента `<null_value>` определенного в настройках словаря.
    - `dictGet[Тип]OrDefault` вернет значение переданного `default_value_expr` параметра.

ClickHouse бросает исключение, если не может обработать значение атрибута или значение несопоставимо с типом атрибута

[Оригинальная статья](https://clickhouse.yandex/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
