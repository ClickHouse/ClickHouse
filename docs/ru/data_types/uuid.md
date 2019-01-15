# UUID {#uuid-data-type}

UUID - это универсально уникальный идентификатор. Он представляет собой 16-байтный (128-битный) номер, который позволяет однозначно идентифицировать записи в таблицах. С детальной информацией об UUID можно ознакомиться на [Википедии](https://en.wikipedia.org/wiki/Universally_unique_identifier).

Ниже приведен пример записи с типом UUID: 

```
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

Если при добавлении UUID записи в таблицу ее значение не определено, то UUID значение будет заполнено нулями:

```
00000000-0000-0000-0000-000000000000
```

## Генерация UUID

Для генерации UUID значений ClickHouse предоставляет функцию [generateUUIDv4](../query_language/functions/uuid_functions.md).

## Примеры использования

**Пример 1**

Пример ниже демонстрирует, как создать таблицу с UUID-колонкой и добавить в нее значение типа UUID.

``` sql
:) CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Пример 2**

В этом примере при добавлении записей в таблицу для UUID-колонки значение не задано. В этом случае UUID будет заполнен нулями. 

``` sql
:) INSERT INTO t_uuid (y) VALUES ('Example 2')

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Ограничения

Тип данных UUID поддерживается только теми функциями, которые умеют работать с типом [String](string.md) (например, [min](../query_language/agg_functions/reference.md#agg_function-min), [max](../query_language/agg_functions/reference.md#agg_function-max) и [count](../query_language/agg_functions/reference.md#agg_function-count)).

UUID не поддерживается арифметическими операциями (например, [abs](../query_language/functions/arithmetic_functions.md#arithm_func-abs)), а также агрегатными функциями, такими как [sum](../query_language/agg_functions/reference.md#agg_function-sum) и [avg](../query_language/agg_functions/reference.md#agg_function-avg).

[Original article](https://clickhouse.yandex/docs/en/data_types/uuid/) <!--hide-->
