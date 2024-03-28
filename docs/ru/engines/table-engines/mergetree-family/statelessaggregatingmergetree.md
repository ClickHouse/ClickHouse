---
slug: /ru/engines/table-engines/mergetree-family/statelessaggregatingmergetree
sidebar_position: 32
sidebar_label:  StatelessAggregatingMergeTree
---

# StatelessAggregatingMergeTree {#statelessaggregatingmergetree}

Движок наследует функционал от  [MergeTree](./mergetree.md#table_engines-mergetree).
Основное отличие заключается в стратегии слияния (merge): `StatelessAggregatingMergeTree` заменяет все строки с одинаковым первичным ключом (точнее, с одинаковым [ключом сортировки](./mergetree.md)) одной строкой, состоящей из агрегированных значений для выбранных столбцов и произвольных значений для остальных.
Если ключ сортировки составлен таким образом, что ему соответствует большое количество строк, это значительно уменьшает объем хранимых данных и ускоряет их выборку.

Мы рекомендуем использовать движок в паре с `MergeTree`: хранить полные данные `MergeTree`, а `StatelessAggregatingMergeTree` использовать для хранения агрегированных данных, например, при подготовке отчетов. Такой подход позволит не утратить ценные данные из-за неправильно выбранного первичного ключа.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StatelessAggregatingMergeTree(aggregate_function(s), [columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Кроме двух дополнительных параметров (см. ниже), процесс создания таблицы не отличается от [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

Более детально о создании таблиц см. [request description](../../../sql-reference/statements/create/table.md).

### Параметры StatelessAggregatingMergeTree

#### aggregate_function(s)

**Обязательный параметр.**

`[aggr|(aggr1, aggr2, ...)]`
Можно указать одну агрегатную функцию, либо кортеж (tuple), содержащий имена агрегатных функций. Каждая агрегатная функция, перечисленная в кортеже, будет применена к соответствующему (по порядку) столбцу, не считая столбца с ключом сортировки.
Если количество указанных агрегатных функций меньше количества агрегируемых столбцов, то для всех последующих столбцов будет использована последняя указанная агрегатная функция.
Если нужна одна агрегатная функция для всех столбцов, можно указать её имя без использования кортежа.

#### columns
**Опциональный параметр.**

`columns` - кортеж с именами столбцов, данные в которых необходимо агрегировать.
Столбцы не должны входить в первичный ключ.
Для столбцов, которые не являются агрегируемыми, берется любое из значений в столбце в рамках того же первичного ключа.

По умолчанию (если параметр не указан) все столбцы, кроме столбца первичного ключа, будут считаться агрегируемыми.

:::note
Тип значения, возвращаемого агрегатной функцией, используемой для агрегации данных в столбце, должен быть таким же, как и тип данных в самом столбце.
В противном случае, вставка данных в такой столбец будет невозможна.
:::

:::warning
Если агрегатная функция не может принимать на вход тип данных, содержащийся в соответствующем столбце, попытки вставки данных в этот столбец приведут к ошибке.
:::

## Пример использования {#usage-example}

Создаём таблицу:

``` sql
CREATE TABLE samt
(
    `k` UInt32,
    `uint64_val` UInt64,
    `int32_val` Int32,
    `nullable_str` Nullable(String)
)
ENGINE = StatelessAggregatingMergeTree((sum, anyLast))
ORDER BY k
```

Вставим немного данных:

``` sql
INSERT INTO samt VALUES (1,1,1,'qwe'),(1,2,2,'rty'),(2,1,1,NULL);
INSERT INTO samt VALUES (1,1,1,'newl'),(2,1,1,'NonNull');
INSERT INTO samt VALUES (2,2,2,NULL);
```

Запросим данные из таблицы:
``` sql
SELECT k, sum(uint64_val), anyLast(int32_val), anyLast(nullable_str) FROM samt GROUP BY k ORDER BY k;
```

``` text
┌─k─┬─sum(uint64_val)─┬─anyLast(int32_val)─┬─anyLast(nullable_str)─┐
│ 1 │               4 │                  1 │ newl                  │
│ 2 │               4 │                  2 │ NonNull               │
└───┴─────────────────┴────────────────────┴───────────────────────┘
```

## Обработка данных {#data-processing}

При вставке данных в таблицу они сначала сохраняются в исходном виде; агрегация происходит в фоновом режиме (merge), и только в этот момент строки с одинаковым первичным ключом заменяются одной строкой с результатом агрегации.

В какой-то момент данные могут быть промежуточно слиты так, что разные парты таблицы могут содержать различные строки с одинаковым первичным ключом. Это означает, что процесс агрегации не окончен. Поэтому лучше всего использовать запросы с соответствующей агрегатной функцией для каждого агрегированного столбца (как в примере выше).
А простой запрос в этом случае `SELECT *` может вернуть неожиданный результат:

```sql
SELECT * FROM samt ORDER BY k;
```

```text
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 2 │          2 │         2 │ ᴺᵁᴸᴸ         │
└───┴────────────┴───────────┴──────────────┘
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 1 │          3 │         2 │ rty          │
│ 2 │          1 │         1 │ ᴺᵁᴸᴸ         │
└───┴────────────┴───────────┴──────────────┘
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 1 │          1 │         1 │ newl         │
│ 2 │          1 │         1 │ NonNull      │
└───┴────────────┴───────────┴──────────────┘
```

### Агрегация столбцов Aggregatefunction {#aggregation-of-aggregatefunction-columns}

Для столбцов с данными типа [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse ведёт себя аналогично движку [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md).