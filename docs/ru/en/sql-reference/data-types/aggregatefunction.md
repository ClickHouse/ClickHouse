---
description: 'Документация по типу данных AggregateFunction в ClickHouse, который
хранит промежуточные состояния агрегатных функций'
keywords: ['AggregateFunction', 'Тип']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'Тип AggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Описание
</div>

Все [агрегатные функции](/ru/sql-reference/aggregate-functions) в ClickHouse имеют
специфичное для реализации промежуточное состояние, которое можно сериализовать в
тип данных `AggregateFunction` и сохранить в таблице. Обычно это делается
с помощью [materialized view](../../sql-reference/statements/create/view.md).

С типом `AggregateFunction` обычно используются два [комбинатора](/ru/sql-reference/aggregate-functions/combinators)
агрегатных функций:

* Комбинатор агрегатных функций [`-State`](/ru/sql-reference/aggregate-functions/combinators#-state), который при добавлении к имени агрегатной
  функции формирует промежуточные состояния `AggregateFunction`.
* Комбинатор агрегатных функций [`-Merge`](/ru/sql-reference/aggregate-functions/combinators#-merge), который используется для получения итогового результата агрегации
  из промежуточных состояний.

<div id="syntax">
  ## Синтаксис
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Параметры**

* `aggregate_function_name` — Название агрегатной функции. Если функция
  параметрическая, следует также указать её параметры.
* `types_of_arguments` — Типы аргументов агрегатной функции.

например:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## Использование
</div>

<div id="data-insertion">
  ### Вставка данных
</div>

Чтобы вставить данные в таблицу со столбцами типа `AggregateFunction`, можно
использовать `INSERT SELECT` с агрегатными функциями и
комбинатором агрегатной функции
[`-State`](/ru/sql-reference/aggregate-functions/combinators#-state).

Например, чтобы вставить данные в столбцы типа `AggregateFunction(uniq, UInt64)` и
`AggregateFunction(quantiles(0.5, 0.9), UInt64)`, используйте следующие
агрегатные функции с комбинаторами.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

В отличие от функций `uniq` и `quantiles`, `uniqState` и `quantilesState`
(с добавленным комбинатором `-State`) возвращают состояние, а не итоговое значение.
Иными словами, они возвращают значение типа `AggregateFunction`.

В результатах запроса `SELECT` значения типа `AggregateFunction` имеют
зависящее от реализации двоичное представление во всех форматах вывода
ClickHouse.

Существует специальная настройка уровня сеанса `aggregate_function_input_format`, которая позволяет формировать состояние из входных значений.
Она поддерживает следующие форматы:

* `state` - двоичная строка с сериализованным состоянием (по умолчанию).
  Если вы выгружаете данные, например, в формате `TabSeparated` с помощью запроса `SELECT`,
  то этот дамп можно затем загрузить обратно с помощью запроса `INSERT`.
* `value` - формат будет ожидать одно значение аргумента агрегатной функции или, в случае нескольких аргументов, кортеж из них; затем оно будет десериализовано для формирования соответствующего состояния
* `array` - формат будет ожидать `Array` значений, как описано выше для варианта `value`; все элементы массива будут агрегированы для формирования состояния

<div id="data-selection">
  ### Выборка данных
</div>

При выборке данных из таблицы `AggregatingMergeTree` используйте предложение `GROUP BY`
и те же агрегатные функции, что и при вставке данных, но с
комбинатором [`-Merge`](/ru/sql-reference/aggregate-functions/combinators#-merge).

Агрегатная функция с добавленным комбинатором `-Merge` принимает набор
состояний, объединяет их и возвращает результат полной агрегации данных.

Например, следующие два запроса возвращают один и тот же результат:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## Пример использования
</div>

См. описание движка [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Использование комбинаторов агрегатных функций в ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* Комбинатор [MergeState](/ru/sql-reference/aggregate-functions/combinators#-mergestate).
* Комбинатор [State](/ru/sql-reference/aggregate-functions/combinators#-state).