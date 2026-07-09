---
alias: []
description: 'Документация по формату Values'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `Values` выводит каждую строку в скобках.

* Строки разделяются запятыми, без запятой после последней строки.
* Значения внутри скобок также разделяются запятыми.
* Числа выводятся в десятичном формате без кавычек.
* Массивы выводятся в `[]`.
* Строки, даты и значения даты и времени выводятся в кавычках.
* Правила экранирования и разбора аналогичны формату [TabSeparated](TabSeparated/TabSeparated.md).

При форматировании дополнительные пробелы не вставляются, но при разборе они допускаются и пропускаются (кроме пробелов внутри значений массива, которые не допускаются).
[`NULL`](/ru/sql-reference/syntax.md) представляется как `NULL`.

Минимальный набор символов, которые нужно экранировать при передаче данных в формате `Values`:

* одинарные кавычки
* обратная косая черта

Этот формат используется в `INSERT INTO t VALUES ...`, но его также можно использовать для форматирования результатов запроса.

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-data">
  ### Вставка данных
</div>

Формат `Values` используется в `INSERT`, поэтому любой оператор `INSERT ... VALUES`
уже работает с ним. Предложение `FORMAT Values` можно указать явно, а
строки можно передавать из потока или файла. Каждая строка представляет собой
заключённый в скобки кортеж, элементы которого разделены запятыми, а сами кортежи также разделены запятыми:

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### Использование выражений во входных данных
</div>

В отличие от большинства входных форматов, `Values` может вычислять SQL-выражения в каждом поле,
а не только принимать литералы. Это поведение контролируется параметром
[`input_format_values_interpret_expressions`](#format-settings) (включен по
умолчанию): если поле не удается прочитать быстрым потоковым парсером, ClickHouse
переключается на SQL-парсер и интерпретирует поле как выражение.

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### Выборка данных
</div>

Формат `Values` также можно использовать для вывода результатов запроса. Числа
записываются без кавычек, массивы — в `[]`, а строки и даты — в одинарных кавычках;
одинарные кавычки и символы обратной косой черты внутри строк экранируются обратной косой чертой, а
[`NULL`](/ru/sql-reference/syntax.md) записывается как `NULL`:

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## Настройки формата
</div>

| Настройка                                                                                                                                                   | Описание                                                                                                                                                                                                 | По умолчанию |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | если поле не удалось разобрать потоковым парсером, запустить SQL-парсер и попытаться интерпретировать его как SQL-выражение.                                                                             | `true`       |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | если поле не удалось разобрать потоковым парсером, запустить SQL-парсер, определить шаблон SQL-выражения, попытаться разобрать все строки по шаблону, а затем интерпретировать выражение для всех строк. | `true`       |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | при разборе и интерпретации выражений с использованием шаблона проверять фактический тип литерала, чтобы избежать возможных проблем с переполнением и точностью.                                         | `true`       |