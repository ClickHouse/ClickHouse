---
sidebar_position: 65
sidebar_label: Map(key, value)
---

# Map(key, value) {#data_type-map}

Тип данных `Map(key, value)` хранит пары `ключ:значение`.

**Параметры**

-   `key` — ключ. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md), [LowCardinality](../../sql-reference/data-types/lowcardinality.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [UUID](../../sql-reference/data-types/uuid.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), [Date32](../../sql-reference/data-types/date32.md), [Enum](../../sql-reference/data-types/enum.md).
-   `value` — значение. Любой тип, включая [Map](../../sql-reference/data-types/map.md) и [Array](../../sql-reference/data-types/array.md).

Чтобы получить значение из колонки `a Map('key', 'value')`, используйте синтаксис `a['key']`. В настоящее время такая подстановка работает по алгоритму с линейной сложностью.

**Примеры**

Рассмотрим таблицу:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
INSERT INTO table_map VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

Выборка всех значений ключа `key2`:

```sql
SELECT a['key2'] FROM table_map;
```
Результат:

```text
┌─arrayElement(a, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

Если для какого-то ключа `key` в колонке с типом `Map()` нет значения, запрос возвращает нули для числовых колонок, пустые строки или пустые массивы.

```sql
INSERT INTO table_map VALUES ({'key3':100}), ({});
SELECT a['key3'] FROM table_map;
```

Результат:

```text
┌─arrayElement(a, 'key3')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
┌─arrayElement(a, 'key3')─┐
│                       0 │
│                       0 │
│                       0 │
└─────────────────────────┘
```

## Подстолбцы Map.keys и Map.values {#map-subcolumns}

Для оптимизации обработки столбцов `Map` в некоторых случаях можно использовать подстолбцы `keys` и `values` вместо чтения всего столбца.

**Пример**

Запрос:

``` sql
CREATE TABLE t_map (`a` Map(String, UInt64)) ENGINE = Memory;

INSERT INTO t_map VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT a.keys FROM t_map;

SELECT a.values FROM t_map;
```

Результат:

``` text
┌─a.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─a.values─┐
│ [1,2,3]  │
└──────────┘
```

**См. также**

-   функция [map()](../../sql-reference/functions/tuple-map-functions.md#function-map)
-   функция [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast)

[Original article](https://clickhouse.com/docs/ru/data-types/map/) <!--hide-->
