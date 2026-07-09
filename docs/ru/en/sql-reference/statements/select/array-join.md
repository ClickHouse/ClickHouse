---
description: 'Документация по предложению ARRAY JOIN'
sidebar_label: 'ARRAY JOIN'
slug: /sql-reference/statements/select/array-join
title: 'Предложение ARRAY JOIN'
doc_type: 'reference'
---

Для таблиц, содержащих столбец типа Array, распространена операция создания новой таблицы, в которой каждому отдельному элементу массива из исходного столбца соответствует своя строка, а значения других столбцов дублируются. Это базовый вариант работы предложения `ARRAY JOIN`.

Название связано с тем, что его можно рассматривать как выполнение `JOIN` с массивом или вложенной структурой данных. По назначению оно похоже на функцию [arrayJoin](/ru/sql-reference/functions/array-join), но возможности предложения шире.

Синтаксис:

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Поддерживаемые типы `ARRAY JOIN` перечислены ниже:

* `ARRAY JOIN` — в общем случае пустые массивы не включаются в результат `JOIN`.
* `LEFT ARRAY JOIN` — результат `JOIN` содержит строки с пустыми массивами. Для пустого массива используется значение по умолчанию для типа его элементов (обычно 0, пустая строка или NULL).

<div id="basic-array-join-examples">
  ## Простые примеры ARRAY JOIN
</div>

<div id="array-join-left-array-join-examples">
  ### ARRAY JOIN и LEFT ARRAY JOIN
</div>

В примерах ниже показано использование предложений `ARRAY JOIN` и `LEFT ARRAY JOIN`. Давайте создадим таблицу со столбцом типа [Array](../../../sql-reference/data-types/array.md) и вставим в неё значения:

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

В примере ниже используется предложение `ARRAY JOIN`:

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

В следующем примере используется предложение `LEFT ARRAY JOIN`:

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

<div id="array-join-arrayEnumerate">
  ### ARRAY JOIN и функция arrayEnumerate
</div>

Эта функция обычно используется с `ARRAY JOIN`. Она позволяет учитывать что-либо только один раз для каждого массива после применения `ARRAY JOIN`. Пример:

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

В этом примере Reaches — это число конверсий (строк, полученных после применения `ARRAY JOIN`), а Hits — число просмотров страниц (строк до `ARRAY JOIN`). В данном случае тот же результат можно получить более простым способом:

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

<div id="array_join_arrayEnumerateUniq">
  ### ARRAY JOIN и arrayEnumerateUniq
</div>

Эта функция полезна при использовании `ARRAY JOIN` и агрегировании элементов массива.

В этом примере для каждого ID цели вычисляются количество конверсий (каждый элемент во вложенной структуре данных Goals — это достигнутая цель, которую мы называем конверсией) и количество сеансов. Без `ARRAY JOIN` мы бы подсчитали количество сеансов как sum(Sign). Но в данном случае строки были размножены вложенной структурой Goals, поэтому, чтобы затем учесть каждый сеанс только один раз, мы применяем условие к значению функции `arrayEnumerateUniq(Goals.ID)`.

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

<div id="using-aliases">
  ## Использование псевдонимов
</div>

В предложении `ARRAY JOIN` для массива можно указать псевдоним. В этом случае к элементу массива можно обращаться по этому псевдониму, а к самому массиву — по исходному имени. Пример:

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

С помощью псевдонимов можно выполнить `ARRAY JOIN` с внешним массивом. Например:

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Несколько массивов можно перечислить через запятую в предложении `ARRAY JOIN`. В этом случае `JOIN` выполняется для них одновременно (прямая сумма, а не декартово произведение). Обратите внимание, что по умолчанию все массивы должны быть одинакового размера. Пример:

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

В примере ниже используется функция [arrayEnumerate](/ru/sql-reference/functions/array-functions#arrayEnumerate):

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

Несколько массивов разного размера можно объединить, задав: `SETTINGS enable_unaligned_array_join = 1`. Пример:

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

<div id="array-join-with-nested-data-structure">
  ## ARRAY JOIN с вложенной структурой данных
</div>

`ARRAY JOIN` также работает с [вложенными структурами данных](../../../sql-reference/data-types/nested-data-structures/index.md):

```sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

При указании имён вложенных структур данных в `ARRAY JOIN` смысл такой же, как у `ARRAY JOIN` со всеми элементами массивов, из которых они состоят. Примеры приведены ниже:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Этот вариант тоже подходит:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Для вложенной структуры данных можно использовать псевдоним, чтобы выбрать либо результат `JOIN`, либо исходный массив. Пример:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Пример использования функции [arrayEnumerate](/ru/sql-reference/functions/array-functions#arrayEnumerate):

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

<div id="implementation-details">
  ## Подробности реализации
</div>

Порядок выполнения запроса оптимизируется при использовании `ARRAY JOIN`. Хотя в запросе `ARRAY JOIN` всегда должен указываться перед предложением [WHERE](../../../sql-reference/statements/select/where.md)/[PREWHERE](../../../sql-reference/statements/select/prewhere.md), фактически они могут выполняться в любом порядке, если только результат `ARRAY JOIN` не используется для фильтрации. Этот порядок определяется оптимизатором запросов.

<div id="incompatibility-with-short-circuit-function-evaluation">
  ### Несовместимость с вычислением функций с коротким замыканием
</div>

[Вычисление функций с коротким замыканием](/ru/operations/settings/settings#short_circuit_function_evaluation) — это возможность, которая оптимизирует выполнение сложных выражений в некоторых функциях, таких как `if`, `multiIf`, `and` и `or`. Она позволяет избежать потенциальных исключений, например деления на ноль, при выполнении этих функций.

`arrayJoin` всегда выполняется и не поддерживает вычисление функций с коротким замыканием. Это связано с тем, что `arrayJoin` — особая функция: на этапе анализа запроса и выполнения она обрабатывается отдельно от всех остальных функций и требует дополнительной логики, несовместимой с вычислением функций с коротким замыканием. Причина в том, что количество строк в результате зависит от результата `arrayJoin`, а реализовать для `arrayJoin` отложенное выполнение слишком сложно и затратно.

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Работа с временными рядами в ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)