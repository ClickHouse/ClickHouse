---
description: 'Документация по запросу SELECT'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'Запрос SELECT'
doc_type: 'reference'
---

Запросы `SELECT` используются для выборки данных. По умолчанию запрошенные данные возвращаются клиенту, а в сочетании с [INSERT INTO](../../../sql-reference/statements/insert-into.md) могут быть перенаправлены в другую таблицу.

<div id="syntax">
  ## Синтаксис
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

Все секции необязательны, за исключением обязательного списка выражений сразу после `SELECT`, который подробнее рассматривается [ниже](#select-clause).

Особенности каждой необязательной секции рассматриваются в отдельных разделах, перечисленных в том же порядке, в котором они выполняются:

* [конструкция WITH](../../../sql-reference/statements/select/with.md)
* [предложение `SELECT`](#select-clause)
* [предложение DISTINCT](../../../sql-reference/statements/select/distinct.md)
* [предложение FROM](../../../sql-reference/statements/select/from.md)
* [предложение SAMPLE](../../../sql-reference/statements/select/sample.md)
* [предложение JOIN](../../../sql-reference/statements/select/join.md)
* [предложение PREWHERE](../../../sql-reference/statements/select/prewhere.md)
* [предложение WHERE](../../../sql-reference/statements/select/where.md)
* [предложение WINDOW](../../../sql-reference/window-functions/index.md)
* [предложение GROUP BY](/ru/sql-reference/statements/select/group-by)
* [предложение LIMIT BY](../../../sql-reference/statements/select/limit-by.md)
* [предложение HAVING](../../../sql-reference/statements/select/having.md)
* [предложение QUALIFY](../../../sql-reference/statements/select/qualify.md)
* [предложение LIMIT](../../../sql-reference/statements/select/limit.md)
* [предложение OFFSET](../../../sql-reference/statements/select/offset.md)
* [предложение UNION](../../../sql-reference/statements/select/union.md)
* [предложение INTERSECT](../../../sql-reference/statements/select/intersect.md)
* [оператор EXCEPT](../../../sql-reference/statements/select/except.md)
* [предложение INTO OUTFILE](../../../sql-reference/statements/select/into-outfile.md)
* [предложение FORMAT](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## Предложение `SELECT`
</div>

[Выражения](/ru/sql-reference/syntax#expressions), указанные в предложении `SELECT`, вычисляются после завершения всех операций в секциях, описанных выше. Эти выражения ведут себя так, как будто применяются к отдельным строкам результата. Если выражения в предложении `SELECT` содержат агрегатные функции, ClickHouse обрабатывает агрегатные функции и выражения, используемые в качестве их аргументов, во время агрегации [GROUP BY](/ru/sql-reference/statements/select/group-by).

Если вы хотите включить в результат все столбцы, используйте символ звёздочки (`*`). Например, `SELECT * FROM ...`.

<div id="dynamic-column-selection">
  ### Динамический выбор столбцов
</div>

Динамический выбор столбцов (также известный как выражение COLUMNS) позволяет выбирать столбцы в результирующем наборе по [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) регулярному выражению.

```sql
COLUMNS('regexp')
```

Например, рассмотрим таблицу:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

Следующий запрос выбирает данные из всех столбцов, в названии которых содержится символ `a`.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Выбранные столбцы возвращаются не в алфавитном порядке.

В одном запросе можно использовать несколько выражений `COLUMNS` и применять к ним функции.

Например:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Каждый столбец, возвращаемый выражением `COLUMNS`, передаётся в функцию как отдельный аргумент. Кроме того, вы можете передавать функции и другие аргументы, если она их поддерживает. Будьте осторожны при использовании функций. Если функция не поддерживает количество переданных ей аргументов, ClickHouse генерирует исключение.

Например:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

В этом примере `COLUMNS('a')` возвращает два столбца: `aa` и `ab`. `COLUMNS('c')` возвращает столбец `bc`. Оператор `+` нельзя применить к 3 аргументам, поэтому ClickHouse генерирует исключение с соответствующим сообщением.

Столбцы, соответствующие выражению `COLUMNS`, могут иметь разные типы данных. Если `COLUMNS` не соответствует ни одному столбцу и является единственным выражением в `SELECT`, ClickHouse генерирует исключение.

<div id="select-columns-with-like-or-ilike">
  #### Выбор столбцов с `LIKE` или `ILIKE`
</div>

Вы также можете выбирать столбцы, сопоставляя их имена с шаблоном после `*`, используя чувствительный к регистру `LIKE` или регистронезависимый `ILIKE`:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Шаблоны `LIKE` и `ILIKE` используют семантику `LIKE`, а не семантику регулярных выражений. Символ `%` соответствует любой последовательности символов, символ `_` — любому одиночному символу, а `\` экранирует `%`, `_` и `\`. Единственное различие между ними в том, что `LIKE` сопоставляет имена столбцов с учетом регистра, а `ILIKE` работает без учета регистра. Например:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

Запрос выбирает столбцы с двухсимвольными именами, начинающимися на `a`, например `aa` и `ab`.

`* LIKE` и `* ILIKE` также поддерживают квалифицированные звёздочки и преобразователи столбцов:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### Звёздочка
</div>

Звёздочку можно использовать в любой части запроса вместо выражения. При разборе запроса звёздочка разворачивается в список всех столбцов таблицы (за исключением столбцов `MATERIALIZED` и `ALIAS`). Есть лишь несколько случаев, когда использование звёздочки оправдано:

* При создании дампа таблицы.
* Для таблиц, содержащих всего несколько столбцов, например системных таблиц.
* Чтобы получить информацию о том, какие столбцы есть в таблице. В этом случае установите `LIMIT 1`. Но лучше использовать запрос `DESC TABLE`.
* Когда по небольшому числу столбцов выполняется сильная фильтрация с помощью `PREWHERE`.
* В подзапросах (поскольку столбцы, не нужные для внешнего запроса, исключаются из подзапросов).

Во всех остальных случаях мы не рекомендуем использовать звёздочку, поскольку она даёт только недостатки столбцовой СУБД, но не её преимущества. Иными словами, использовать звёздочку не рекомендуется.

<div id="extreme-values">
  ### Экстремальные значения
</div>

Помимо результатов, можно также получить минимальные и максимальные значения для столбцов результата. Для этого установите значение настройки **extremes** в 1. Минимальные и максимальные значения вычисляются для числовых типов, дат и значений даты и времени. Для остальных столбцов выводятся значения по умолчанию.

Дополнительно вычисляются две строки — с минимальными и максимальными значениями соответственно. Эти две дополнительные строки выводятся в [форматах](../../../interfaces/formats.md) `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` и `Pretty*` отдельно от остальных строк. Для других форматов они не выводятся.

В форматах `JSON*` и `XML` экстремальные значения выводятся в отдельном поле `extremes`. В форматах `TabSeparated*`, `CSV*` и `Vertical` эта строка выводится после основного результата, а при наличии — и после `totals`. Перед ней выводится пустая строка (после остальных данных). В форматах `Pretty*` строка выводится в виде отдельной таблицы после основного результата, а при наличии — и после `totals`. В формате `Template` экстремальные значения выводятся в соответствии с указанным шаблоном.

Экстремальные значения вычисляются для строк до `LIMIT`, но после `LIMIT BY`. Однако при использовании `LIMIT offset, size` строки до `offset` включаются в `extremes`. В потоковых запросах результат также может включать небольшое количество строк, прошедших через `LIMIT`.

<div id="notes">
  ### Примечания
</div>

Вы можете использовать синонимы (псевдонимы с `AS`) в любой части запроса.

Секции `GROUP BY`, `ORDER BY` и `LIMIT BY` поддерживают позиционные аргументы. Чтобы включить эту возможность, активируйте настройку [enable&#95;positional&#95;arguments](/ru/operations/settings/settings#enable_positional_arguments). Тогда, например, `ORDER BY 1,2` будет сортировать строки в таблице сначала по первому, а затем по второму столбцу.

<div id="implementation-details">
  ## Подробности реализации
</div>

Если запрос не содержит секций `DISTINCT`, `GROUP BY` и `ORDER BY`, а также подзапросов `IN` и `JOIN`, он будет полностью обрабатываться в потоковом режиме с использованием O(1) оперативной памяти. В противном случае запрос может потреблять большой объём оперативной памяти, если не заданы соответствующие ограничения:

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

Дополнительные сведения см. в разделе &quot;Settings&quot;. Можно использовать внешнюю сортировку (с сохранением временных таблиц на диск) и внешнюю агрегацию.

<div id="select-modifiers">
  ## Модификаторы SELECT
</div>

В запросах `SELECT` можно использовать следующие модификаторы.

| Модификатор                        | Описание                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`APPLY`](./apply_modifier.md)     | Позволяет применить функцию к каждой строке, возвращаемой внешним табличным выражением запроса.                                                                                                                                                                                                                                                                                          |
| [`EXCEPT`](./except_modifier.md)   | Указывает имена одного или нескольких столбцов, которые нужно исключить из результата. Все совпадающие имена столбцов не включаются в вывод.                                                                                                                                                                                                                                             |
| [`REPLACE`](./replace_modifier.md) | Указывает один или несколько [псевдонимов выражений](/ru/sql-reference/syntax#expression-aliases). Каждый псевдоним должен совпадать с именем столбца из оператора `SELECT *`. В списке выходных столбцов столбец, соответствующий псевдониму, заменяется выражением из `REPLACE`. Этот модификатор не изменяет имена и порядок столбцов. Однако он может изменить значение и тип значения. |

<div id="modifier-combinations">
  ### Сочетания модификаторов
</div>

Можно использовать каждый модификатор по отдельности или сочетать их.

**Примеры:**

Многократное использование одного и того же модификатора.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Использование нескольких модификаторов в одном запросе.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SETTINGS в запросе SELECT
</div>

Вы можете указать нужные настройки прямо в запросе `SELECT`. Значение настройки применяется только к этому запросу, а после его выполнения сбрасывается к `default` или к предыдущему значению.

О других способах задать настройки см. [здесь](/ru/operations/settings/overview).

Для булевых настроек со значением true можно использовать сокращённый синтаксис, опуская присваивание значения. Если указано только имя настройки, ей автоматически присваивается значение `1` (true).

**Пример**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```