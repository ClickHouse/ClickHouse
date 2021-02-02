---
toc_priority: 65
toc_title: Map(key, value)
---

# Map(key, value) {#data_type-map}

Тип данных `Map(key, value)` хранит пары `ключ:значение` в структурах типа JSON. 

**Параметры** 
-   `key` — ключ. [String](../../sql-reference/data-types/string.md) или [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — значение. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) или [Array](../../sql-reference/data-types/array.md).

!!! warning "Предупреждение"
    Сейчас использование типа данных `Map` является экспериментальной возможностью. Чтобы использовать этот тип данных, включите настройку `allow_experimental_map_type = 1`.

Чтобы получить значение из колонки `a Map('key', 'value')`, используйте синтаксис `a['key']`.

**Пример**

Запрос:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE=Memory;
INSERT INTO table_map VALUES ({'key1':1, 'key2':100}), ({'key1':2,'key2':200}), ({'key1':3,'key2':300});
SELECT a['key2'] FROM table_map;
```
Результат:

```text
┌─arrayElement(a, 'key2')─┐
│                     100 │
│                     200 │
│                     300 │
└─────────────────────────┘
```

## Преобразование типа данных Tuple в Map {#map-and-tuple}

Для преобразования данных с типом `Tuple()` в тип `Map()` можно использовать функцию [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast):

``` sql
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

``` text
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

**См. также**

-   функция [map()](../../sql-reference/functions/tuple-map-functions.md#function-map)
-   функция [CAST()](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast)

[Original article](https://clickhouse.tech/docs/ru/data-types/map/) <!--hide-->
