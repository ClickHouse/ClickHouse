---
description: 'Документация по оператору JOIN'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'Оператор JOIN'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'reference'
---

Оператор `JOIN` создает новую таблицу, объединяя столбцы из одной или нескольких таблиц по общим значениям. Это распространенная операция в базах данных с поддержкой SQL, соответствующая операции JOIN в [реляционной алгебре](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators). Частный случай, когда таблица объединяется сама с собой, часто называют &quot;self-join&quot;.

**Синтаксис**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Выражения из предложения `ON` и столбцы из предложения `USING` называются &quot;ключами JOIN&quot;. Если не указано иное, `JOIN` формирует [декартово произведение](https://en.wikipedia.org/wiki/Cartesian_product) для строк с совпадающими &quot;ключами JOIN&quot;, из-за чего в результате может получиться гораздо больше строк, чем в исходных таблицах.

<div id="supported-types-of-join">
  ## Поддерживаемые типы JOIN
</div>

Поддерживаются все стандартные типы [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)):

| Тип                | Описание                                                                                                                                                                                                                                                                                                                     |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | возвращаются только совпадающие строки.                                                                                                                                                                                                                                                                                      |
| `LEFT OUTER JOIN`  | помимо совпадающих строк возвращаются несовпадающие строки из левой таблицы.                                                                                                                                                                                                                                                 |
| `RIGHT OUTER JOIN` | помимо совпадающих строк возвращаются несовпадающие строки из правой таблицы.                                                                                                                                                                                                                                                |
| `FULL OUTER JOIN`  | помимо совпадающих строк возвращаются несовпадающие строки из обеих таблиц.                                                                                                                                                                                                                                                  |
| `CROSS JOIN`       | создаёт декартово произведение всех таблиц; «ключи JOIN» **не** указываются.                                                                                                                                                                                                                                                 |
| `NATURAL JOIN`     | автоматически выполняет JOIN по всем столбцам с одинаковыми именами в обеих таблицах; каждый общий столбец появляется в результате только один раз. Поддерживает варианты `INNER` (по умолчанию), `LEFT`, `RIGHT` и `FULL`. Эквивалентно `JOIN ... USING (col1, col2, ...)`, где список столбцов определяется автоматически. |

* `JOIN` без указания типа подразумевает `INNER`.
* Ключевое слово `OUTER` можно без ограничений опускать.
* Альтернативный синтаксис для `CROSS JOIN` — указать несколько таблиц в [`предложении FROM`](../../../sql-reference/statements/select/from.md), разделив их запятыми.
* Если для `NATURAL JOIN` нет совпадающих столбцов, он работает как `CROSS JOIN`.

Дополнительные типы JOIN, доступные в ClickHouse:

| Тип                                                 | Описание                                                                                                                                                    |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | Список разрешённых значений по «ключам JOIN», без создания декартова произведения.                                                                          |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | Список запрещённых значений по «ключам JOIN», без создания декартова произведения.                                                                          |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Частично (для противоположной стороны `LEFT` и `RIGHT`) или полностью (для `INNER` и `FULL`) отключает декартово произведение для стандартных типов `JOIN`. |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | Соединение последовательностей с неточным совпадением. Использование `ASOF JOIN` описано ниже.                                                              |
| `PASTE JOIN`                                        | Выполняет горизонтальную конкатенацию двух таблиц.                                                                                                          |

:::note
Когда [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) установлено в `partial_merge`, `RIGHT JOIN` и `FULL JOIN` поддерживаются только со строгостью `ALL` (`SEMI`, `ANTI`, `ANY` и `ASOF` не поддерживаются).
:::

<div id="settings">
  ## Настройки
</div>

Тип JOIN по умолчанию можно переопределить с помощью настройки [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness).

Поведение сервера ClickHouse для операций `ANY JOIN` зависит от настройки [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys).

**См. также**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

Используйте настройку `cross_to_inner_join_rewrite`, чтобы определить поведение, если ClickHouse не удаётся переписать `CROSS JOIN` в `INNER JOIN`. Значение по умолчанию — `1`: в этом случае выполнение JOIN продолжается, но он будет работать медленнее. Установите `cross_to_inner_join_rewrite` в `0`, если хотите, чтобы было сгенерировано исключение, и в `2`, чтобы не выполнять comma/cross joins, а вместо этого принудительно переписывать их все. Если переписывание не удастся при значении `2`, вы получите сообщение об ошибке: &quot;Please, try to simplify `WHERE` section&quot;.

<div id="on-section-conditions">
  ## Условия в разделе `ON`
</div>

Раздел `ON` может содержать несколько условий, объединённых операторами `AND` и `OR`. Условия, задающие ключи JOIN, должны:

* ссылаться и на левую, и на правую таблицы
* использовать оператор равенства

Остальные условия могут использовать другие логические операторы, но должны ссылаться либо на левую, либо на правую таблицу запроса.

Строки соединяются, если выполняется всё составное условие. Если условия не выполняются, строки всё равно могут попасть в результат в зависимости от типа `JOIN`. Обратите внимание: если те же условия поместить в предложение `WHERE` и они не выполняются, строки всегда исключаются из результата.

Оператор `OR` внутри предложения `ON` работает с использованием алгоритма hash join — для каждого аргумента `OR` с ключами JOIN в `JOIN` создаётся отдельная хеш-таблица, поэтому потребление памяти и время выполнения запроса линейно растут с увеличением числа выражений `OR` в предложении `ON`.

:::note
Если условие ссылается на столбцы из разных таблиц, то на данный момент поддерживается только оператор равенства (`=`).
:::

**Пример**

Рассмотрим `table_1` и `table_2`:

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Запрос с одним условием по ключу JOIN и дополнительным условием для `table_2`:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Обратите внимание, что результат содержит строку с именем `C` и пустым текстовым столбцом. Она включена в результат, поскольку используется JOIN типа `OUTER`.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Запрос с JOIN типа `INNER` и несколькими условиями:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

Запрос с JOIN типа `INNER` и условием с `OR`:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Запрос с JOIN типа `INNER` и условиями с `OR` и `AND`:

:::note

По умолчанию поддерживаются условия с неравенством, если в них используются столбцы из одной и той же таблицы.
Например, `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, поскольку в `t1.b > 0` используются только столбцы из `t1`, а в `t2.b > t2.c` — только из `t2`.
Однако вы можете попробовать экспериментальную поддержку условий вида `t1.a = t2.key AND t1.b > t2.key`; подробнее см. в разделе ниже.

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## JOIN с условиями неравенства для столбцов из разных таблиц
</div>

В настоящее время ClickHouse поддерживает `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` с условиями неравенства наряду с условиями равенства. Условия неравенства поддерживаются только для алгоритмов JOIN `hash` и `grace_hash`. Условия неравенства не поддерживаются при использовании `join_use_nulls`.

**Пример**

Таблица `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

Таблица `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## Значения NULL в ключах JOIN
</div>

`NULL` не равен никакому значению, включая самого себя. Это означает, что если ключ `JOIN` имеет значение `NULL` в одной таблице, он не будет совпадать со значением `NULL` в другой таблице.

**Пример**

Таблица `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

Таблица `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

Обратите внимание, что строка с `Charlie` из таблицы `A` и строка с оценкой 88 из таблицы `B` отсутствуют в результате из-за значения `NULL` в ключе `JOIN`.

Если нужно сопоставлять значения `NULL`, используйте функцию `isNotDistinctFrom` для сравнения ключей `JOIN`.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## Использование ASOF JOIN
</div>

`ASOF JOIN` полезен, когда нужно объединить записи, для которых нет точного совпадения.

Для этого алгоритма JOIN в таблицах требуется специальный столбец. Этот столбец:

* Должен содержать упорядоченную последовательность.
* Может иметь один из следующих типов: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md).
* Для алгоритма `hash` JOIN он не может быть единственным столбцом в секции `JOIN`.

Синтаксис `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Можно использовать любое количество условий равенства и ровно одно условие ближайшего соответствия. Например, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Для ближайшего соответствия поддерживаются следующие условия: `>`, `>=`, `<`, `<=`.

Синтаксис `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` использует `equi_columnX` для JOIN по равенству и `asof_column` для JOIN по ближайшему совпадению с условием `table_1.asof_column >= table_2.asof_column`. Столбец `asof_column` всегда указывается последним в предложении `USING`.

Например, рассмотрим следующие таблицы:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` может брать временную метку пользовательского события из `table_1` и находить событие в `table_2`, временная метка которого ближе всего к временной метке события из `table_1` и соответствует условию ближайшего совпадения. Если доступны одинаковые значения временных меток, они считаются ближайшими. Здесь столбец `user_id` можно использовать для JOIN по равенству, а столбец `ev_time` — для JOIN по ближайшему совпадению. В нашем примере `event_1_1` можно объединить с `event_2_1`, а `event_1_2` — с `event_2_3`, но `event_2_2` объединить нельзя.

:::note
`ASOF JOIN` поддерживается только алгоритмами JOIN `hash` и `full_sorting_merge`.
Он **не** поддерживается в движке таблицы [Join](../../../engines/table-engines/special/join.md).
:::

<div id="paste-join-usage">
  ## Использование PASTE JOIN
</div>

Результатом `PASTE JOIN` является таблица, содержащая все столбцы из левого подзапроса, а затем все столбцы из правого подзапроса.
Строки сопоставляются по их позициям в исходных таблицах (порядок строк должен быть определён).
Если подзапросы возвращают разное количество строк, лишние строки будут отброшены.

Пример:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

Note: в этом случае результат может быть недетерминированным при параллельном чтении. Например:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## Distributed JOIN
</div>

Существует два способа выполнить JOIN с участием distributed таблиц:

* При использовании обычного `JOIN` запрос отправляется на удалённые серверы. На каждом из них выполняются подзапросы для формирования правой таблицы, после чего выполняется JOIN с этой таблицей. Иными словами, правая таблица формируется отдельно на каждом сервере.
* При использовании `GLOBAL ... JOIN` сервер-инициатор сначала выполняет подзапрос, чтобы вычислить одну из сторон JOIN, и сохраняет результат во временную таблицу. Затем эта временная таблица передаётся на каждый удалённый сервер, и на них выполняются запросы с использованием переданных временных данных. Для `LEFT` и `INNER` JOIN правая таблица вычисляется как подзапрос. Для `RIGHT` JOIN вместо неё вычисляется левая таблица, поскольку сохраняется именно правая таблица и она должна считываться из сегментов.

Будьте осторожны при использовании `GLOBAL`. Дополнительную информацию см. в разделе [Распределённые подзапросы](/ru/sql-reference/operators/in#distributed-subqueries).

<div id="implicit-type-conversion">
  ## Неявное преобразование типов
</div>

Запросы `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` и `FULL JOIN` поддерживают неявное преобразование типов для &quot;ключей JOIN&quot;. Однако запрос не может быть выполнен, если ключи JOIN из левой и правой таблиц невозможно преобразовать к одному типу (например, не существует типа данных, который мог бы содержать все значения и из `UInt64`, и из `Int64`, либо из `String`, и из `Int32`).

**Пример**

Рассмотрим таблицу `t_1`:

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

и таблица `t_2`:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

Запрос

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

возвращает множество:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## Рекомендации по использованию
</div>

<div id="processing-of-empty-or-null-cells">
  ### Обработка пустых ячеек или ячеек со значением NULL
</div>

При объединении таблиц могут появляться пустые ячейки. Параметр [join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) определяет, как ClickHouse заполняет эти ячейки.

Если ключи `JOIN` — это поля [Nullable](../../../sql-reference/data-types/nullable.md), то строки, в которых хотя бы один из ключей имеет значение [NULL](/ru/sql-reference/syntax#null), не объединяются.

<div id="syntax">
  ### Синтаксис
</div>

Столбцы, указанные в `USING`, должны иметь одинаковые имена в обоих подзапросах, а остальные столбцы должны называться по-разному. Чтобы изменить имена столбцов в подзапросах, можно использовать псевдонимы.

Предложение `USING` задает один или несколько столбцов для JOIN, устанавливая равенство этих столбцов. Список столбцов указывается без скобок. Более сложные условия JOIN не поддерживаются.

<div id="syntax-limitations">
  ### Ограничения синтаксиса
</div>

При использовании нескольких секций `JOIN` в одном запросе `SELECT`:

* Выбрать все столбцы через `*` можно только при JOIN таблиц, но не подзапросов.
* Предложение `PREWHERE` недоступно.
* Предложение `USING` недоступно.

Для секций `ON`, `WHERE` и `GROUP BY`:

* В секциях `ON`, `WHERE` и `GROUP BY` нельзя использовать произвольные выражения, но можно определить выражение в секции `SELECT`, а затем использовать его в этих секциях через псевдоним.

<div id="performance">
  ### Производительность
</div>

При выполнении `JOIN` порядок выполнения относительно других этапов запроса не оптимизируется. `JOIN` (поиск в правой таблице) выполняется до фильтрации в предложении `WHERE` и до агрегации.

Каждый раз, когда выполняется запрос с одним и тем же `JOIN`, подзапрос запускается заново, поскольку результат не кэшируется. Чтобы этого избежать, используйте специальный движок таблицы [Join](../../../engines/table-engines/special/join.md), который представляет собой подготовленный для JOIN массив, всегда находящийся в оперативной памяти.

В некоторых случаях эффективнее использовать [IN](../../../sql-reference/operators/in.md) вместо `JOIN`.

Если `JOIN` нужен для соединения с таблицами-измерениями (это относительно небольшие таблицы, содержащие свойства измерений, например названия рекламных кампаний), `JOIN` может быть не очень удобен, поскольку к правой таблице приходится повторно обращаться для каждого запроса. В таких случаях вместо `JOIN` следует использовать возможность &quot;Dictionaries&quot;. Подробнее см. в разделе [Dictionaries](/ru/sql-reference/statements/create/dictionary/overview.md).

<div id="memory-limitations">
  ### Ограничения памяти
</div>

По умолчанию ClickHouse использует алгоритм [hash join](https://en.wikipedia.org/wiki/Hash_join). ClickHouse берет `right_table` и создает для нее хеш-таблицу в оперативной памяти. Если включен параметр `join_algorithm = 'auto'`, то после достижения определенного порога потребления памяти ClickHouse переключается на алгоритм [merge](https://en.wikipedia.org/wiki/Sort-merge_join) join. Описание алгоритмов `JOIN` см. в настройке [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm).

Если вам нужно ограничить потребление памяти операцией `JOIN`, используйте следующие настройки:

* [max&#95;rows&#95;in&#95;join](/ru/operations/settings/settings#max_rows_in_join) — Ограничивает количество строк в хеш-таблице.
* [max&#95;bytes&#95;in&#95;join](/ru/operations/settings/settings#max_bytes_in_join) — Ограничивает размер хеш-таблицы.

Когда достигается любой из этих пределов, ClickHouse действует в соответствии с настройкой [join&#95;overflow&#95;mode](/ru/operations/settings/settings#join_overflow_mode).

<div id="examples">
  ## Примеры
</div>

Пример:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [ClickHouse: сверхбыстрая СУБД с полной поддержкой SQL JOIN — Часть 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* Блог: [ClickHouse: сверхбыстрая СУБД с полной поддержкой SQL JOIN — внутреннее устройство — Часть 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* Блог: [ClickHouse: сверхбыстрая СУБД с полной поддержкой SQL JOIN — внутреннее устройство — Часть 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* Блог: [ClickHouse: сверхбыстрая СУБД с полной поддержкой SQL JOIN — внутреннее устройство — Часть 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)