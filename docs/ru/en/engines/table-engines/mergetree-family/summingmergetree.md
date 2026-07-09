---
description: 'SummingMergeTree наследует от движка MergeTree. Его ключевая особенность
  — возможность автоматически суммировать числовые данные во время слияния частей.'
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'Движок таблицы SummingMergeTree'
doc_type: 'reference'
---

Движок наследуется от [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree). Отличие в том, что при слиянии частей данных для таблиц `SummingMergeTree` ClickHouse заменяет все строки с одинаковым первичным ключом (или, точнее, с одинаковым [ключом сортировки](../../../engines/table-engines/mergetree-family/mergetree.md)) одной строкой, содержащей суммированные значения в столбцах с числовым типом данных. Если ключ сортировки составлен так, что одному значению ключа соответствует большое количество строк, это значительно уменьшает объем хранилища и ускоряет выборку данных.

Мы рекомендуем использовать этот движок вместе с `MergeTree`. Храните полные данные в таблице `MergeTree`, а `SummingMergeTree` используйте для хранения агрегированных данных, например при подготовке отчетов. Такой подход позволит избежать потери ценных данных из-за неправильно составленного первичного ключа.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Сведения о параметрах запроса см. в разделе [описание запроса](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-summingmergetree">
  ### Параметры SummingMergeTree
</div>

<div id="columns">
  #### Столбцы
</div>

`columns` — кортеж с именами столбцов, в которых будут суммироваться значения. Необязательный параметр.
Столбцы должны иметь числовой тип данных и не должны входить в партицию или ключ сортировки.

Если `columns` не указан, ClickHouse суммирует значения во всех столбцах с числовым типом данных, не входящих в ключ сортировки.

<div id="query-clauses">
  ### Секции запроса
</div>

При создании таблицы `SummingMergeTree` требуются те же [секции](../../../engines/table-engines/mergetree-family/mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">
  <summary>Устаревший метод создания таблицы</summary>

  :::note
  Не используйте этот метод в новых проектах и, по возможности, переведите старые проекты на метод, описанный выше.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Все параметры, кроме `columns`, имеют тот же смысл, что и в `MergeTree`.

  * `columns` — кортеж с именами столбцов, значения которых будут суммироваться. Необязательный параметр. Описание см. выше.
</details>

<div id="usage-example">
  ## Пример использования
</div>

Рассмотрим следующую таблицу:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Вставьте в неё данные:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse может суммировать строки не полностью ([см. ниже](#data-processing)), поэтому в запросе мы используем агрегатную функцию `sum` и предложение `GROUP BY`.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

<div id="data-processing">
  ## Обработка данных
</div>

Когда данные вставляются в таблицу, они сохраняются в исходном виде. ClickHouse периодически выполняет слияние вставленных частей данных, и именно в этот момент строки с одинаковым первичным ключом суммируются и заменяются одной строкой в каждой результирующей части данных.

ClickHouse может выполнять слияние частей данных так, что в разных результирующих частях данных могут оказаться строки с одинаковым первичным ключом, то есть суммирование будет неполным. Поэтому в запросе (`SELECT`) следует использовать агрегатную функцию [sum()](/ru/sql-reference/aggregate-functions/reference/sum) и предложение `GROUP BY`, как описано в примере выше.

<div id="common-rules-for-summation">
  ### Общие правила суммирования
</div>

Значения в столбцах с числовым типом данных суммируются. Набор столбцов задаётся параметром `columns`.

Если во всех суммируемых столбцах значения равны 0, строка удаляется.

Если столбец не входит в первичный ключ и не суммируется, для него выбирается произвольное из существующих значений.

Для столбцов, входящих в первичный ключ, значения не суммируются.

<div id="the-summation-in-the-aggregatefunction-columns">
  ### Суммирование в столбцах AggregateFunction
</div>

Для столбцов типа [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse работает как движок [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md), выполняя агрегирование в соответствии с функцией.

<div id="nested-structures">
  ### Вложенные структуры
</div>

Таблица может содержать вложенные структуры данных, которые обрабатываются особым образом.

Если имя вложенной таблицы оканчивается на `Map` и она содержит как минимум два столбца, соответствующих следующим критериям:

* первый столбец — числовой `(*Int*, Date, DateTime)` или строковый `(String, FixedString)`, назовём его `key`,
* остальные столбцы — арифметические `(*Int*, Float32/64)`, назовём их `(values...)`,

то такая вложенная таблица интерпретируется как отображение `key => (values...)`, а при слиянии её строк элементы двух наборов данных объединяются по `key` с суммированием соответствующих `(values...)`.

Примеры:

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

При запросе данных используйте функцию [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) для агрегации значений типа `Map`.

Для вложенной структуры данных её столбцы не нужно указывать в кортеже столбцов для суммирования.

<div id="tuple-element-aggregation">
  ### Агрегация элементов Tuple
</div>

Когда настройка `allow_tuple_element_aggregation` включена, столбцы `Tuple` рекурсивно преобразуются в плоскую структуру, так что каждый конечный элемент независимо участвует в суммировании. Это позволяет хранить несколько метрик в одном столбце `Tuple` и суммировать их поэлементно во время слияний.

К полученным подстолбцам применяются те же правила, что и к обычным столбцам:

* Суммируются только числовые подстолбцы.
* Подстолбцы, относящиеся к `Tuple` в ключе сортировки или ключе партиционирования, исключаются из суммирования.
* Если указан `columns`, суммируются только подстолбцы перечисленных столбцов `Tuple`.
* Если после суммирования все числовые подстолбцы строки равны нулю, строка удаляется.

:::note
Эта настройка неизменяема и должна быть задана при создании таблицы.
:::

```sql
CREATE TABLE summing_tuples
(
    key UInt32,
    metrics Tuple(
        impressions UInt64,
        clicks UInt64,
        nested Tuple(
            conversions UInt64
        )
    )
) ENGINE = SummingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO summing_tuples VALUES (1, (100, 10, (1)));
INSERT INTO summing_tuples VALUES (1, (200, 20, (3)));

OPTIMIZE TABLE summing_tuples FINAL;

SELECT key, metrics.impressions, metrics.clicks, metrics.nested.conversions FROM summing_tuples;
```

```text
┌─key─┬─metrics.impressions─┬─metrics.clicks─┬─metrics.nested.conversions─┐
│   1 │                 300 │             30 │                          4 │
└─────┴─────────────────────┴────────────────┴────────────────────────────┘
```

<div id="related-content">
  ## Материалы по теме
</div>

* Блог: [Агрегатные комбинаторы в ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)