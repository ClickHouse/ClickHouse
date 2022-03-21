---
toc_title: JOIN
---

# Секция JOIN {#select-join}

`JOIN` создаёт новую таблицу путем объединения столбцов из одной или нескольких таблиц с использованием общих для каждой из них значений. Это обычная операция в базах данных с поддержкой SQL, которая соответствует join из [реляционной алгебры](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators). Частный случай соединения одной таблицы часто называют self-join.

**Синтаксис**

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Выражения из секции `ON` и столбцы из секции `USING`  называется «ключами соединения». Если не указано иное, при присоединение создаётся [Декартово произведение](https://en.wikipedia.org/wiki/Cartesian_product) из строк с совпадающими значениями ключей соединения, что может привести к получению результатов с гораздо большим количеством строк, чем исходные таблицы.

## Поддерживаемые типы соединения {#select-join-types}

Все типы из стандартного [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) поддерживаются:

-   `INNER JOIN`, возвращаются только совпадающие строки.
-   `LEFT OUTER JOIN`, не совпадающие строки из левой таблицы возвращаются в дополнение к совпадающим строкам.
-   `RIGHT OUTER JOIN`, не совпадающие строки из правой таблицы возвращаются в дополнение к совпадающим строкам.
-   `FULL OUTER JOIN`, не совпадающие строки из обеих таблиц возвращаются в дополнение к совпадающим строкам.
-   `CROSS JOIN`, производит декартово произведение таблиц целиком, ключи соединения не указываются.

Без указания типа `JOIN` подразумевается `INNER`. Ключевое слово `OUTER` можно опускать. Альтернативным синтаксисом для `CROSS JOIN` является ли указание нескольких таблиц, разделённых запятыми, в [секции FROM](from.md).

Дополнительные типы соединений, доступные в ClickHouse:

-   `LEFT SEMI JOIN` и `RIGHT SEMI JOIN`, белый список по ключам соединения, не производит декартово произведение.
-   `LEFT ANTI JOIN` и `RIGHT ANTI JOIN`, черный список по ключам соединения, не производит декартово произведение.
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` и `INNER ANY JOIN`, Частично (для противоположных сторон `LEFT` и `RIGHT`) или полностью (для `INNER` и `FULL`) отключает декартово произведение для стандартных видов `JOIN`.
-   `ASOF JOIN` и `LEFT ASOF JOIN`, Для соединения последовательностей по нечеткому совпадению. Использование `ASOF JOIN` описано ниже.

!!! note "Примечание"
    Если настройка [join_algorithm](../../../operations/settings/settings.md#settings-join_algorithm) установлена в значение `partial_merge`, то для `RIGHT JOIN` и `FULL JOIN` поддерживается только уровень строгости `ALL` (`SEMI`, `ANTI`, `ANY` и `ASOF` не поддерживаются).

## Настройки {#join-settings}

Значение строгости по умолчанию может быть переопределено с помощью настройки [join_default_strictness](../../../operations/settings/settings.md#settings-join_default_strictness).

Поведение сервера ClickHouse для операций `ANY JOIN` зависит от параметра [any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys).

**См. также**

- [join_algorithm](../../../operations/settings/settings.md#settings-join_algorithm)
- [join_any_take_last_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)
- [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)
- [partial_merge_join_optimizations](../../../operations/settings/settings.md#partial_merge_join_optimizations)
- [partial_merge_join_rows_in_right_blocks](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
- [join_on_disk_max_files_to_merge](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
- [any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

## Условия в секции ON {#on-section-conditions}

Секция `ON` может содержать несколько условий, связанных операторами `AND` и `OR`. Условия, задающие ключи соединения, должны содержать столбцы левой и правой таблицы и должны использовать оператор равенства. Прочие условия могут использовать другие логические операторы, но в отдельном условии могут использоваться столбцы либо только левой, либо только правой таблицы.

Строки объединяются только тогда, когда всё составное условие выполнено. Если оно не выполнено, то строки могут попасть в результат в зависимости от типа `JOIN`. Обратите внимание, что если то же самое условие поместить в секцию `WHERE`, то строки, для которых оно не выполняется, никогда не попаду в результат.

Оператор `OR` внутри секции `ON` работает, используя алгоритм хеш-соединения — на каждый агрумент `OR` с ключами соединений для `JOIN` создается отдельная хеш-таблица, поэтому потребление памяти и время выполнения запроса растет линейно при увеличении количества выражений `OR` секции `ON`.

!!! note "Примечание"
    Если в условии использованы столбцы из разных таблиц, то пока поддерживается только оператор равенства (`=`).

**Пример**

Рассмотрим `table_1` и `table_2`:

```
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Запрос с одним условием, задающим ключ соединения, и дополнительным условием для `table_2`:

``` sql
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2 
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Обратите внимание, что результат содержит строку с именем `C` и пустым текстом. Строка включена в результат, потому что использован тип соединения `OUTER`.

```
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Запрос с типом соединения `INNER` и несколькими условиями:

``` sql
SELECT name, text, scores FROM table_1 INNER JOIN table_2 
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

Результат:

```
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

Запрос с типом соединения `INNER` и условием с оператором `OR`:

``` sql
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

Результат:

```
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Запрос с типом соединения `INNER` и условиями с операторами `OR` и `AND`:

``` sql
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

Результат:

```
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```
## Использование ASOF JOIN {#asof-join-usage}

`ASOF JOIN` применим в том случае, когда необходимо объединять записи, которые не имеют точного совпадения.

Для работы алгоритма необходим специальный столбец в таблицах. Этот столбец:

- Должен содержать упорядоченную последовательность.
- Может быть одного из следующих типов: [Int, UInt](../../data-types/int-uint.md), [Float](../../data-types/float.md), [Date](../../data-types/date.md), [DateTime](../../data-types/datetime.md), [Decimal](../../data-types/decimal.md).
- Не может быть единственным столбцом в секции `JOIN`.

Синтаксис `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Можно использовать произвольное количество условий равенства и одно условие на ближайшее совпадение. Например, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Условия, поддержанные для проверки на ближайшее совпадение: `>`, `>=`, `<`, `<=`.

Синтаксис `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

Для слияния по равенству `ASOF JOIN` использует `equi_columnX`, а для слияния по ближайшему совпадению использует `asof_column` с условием `table_1.asof_column >= table_2.asof_column`. Столбец `asof_column` должен быть последним в секции `USING`.

Например, рассмотрим следующие таблицы:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` принимает метку времени пользовательского события из `table_1` и находит такое событие в `table_2` метка времени которого наиболее близка к метке времени события из `table_1` в соответствии с условием на ближайшее совпадение. При этом столбец `user_id` используется для объединения по равенству, а столбец `ev_time` для объединения по ближайшему совпадению. В нашем примере `event_1_1` может быть объединено с `event_2_1`, `event_1_2` может быть объединено с `event_2_3`, а `event_2_2` не объединяется.

!!! note "Примечание"
    `ASOF JOIN` не поддержан для движка таблиц [Join](../../../engines/table-engines/special/join.md).

Чтобы задать значение строгости по умолчанию, используйте сессионный параметр [join_default_strictness](../../../operations/settings/settings.md#settings-join_default_strictness).

## Распределённый JOIN {#global-join}

Есть два пути для выполнения соединения с участием распределённых таблиц:

-   При использовании обычного `JOIN` , запрос отправляется на удалённые серверы. На каждом из них выполняются подзапросы для формирования «правой» таблицы, и с этой таблицей выполняется соединение. То есть, «правая» таблица формируется на каждом сервере отдельно.
-   При использовании `GLOBAL ... JOIN`, сначала сервер-инициатор запроса запускает подзапрос для вычисления правой таблицы. Эта временная таблица передаётся на каждый удалённый сервер, и на них выполняются запросы с использованием переданных временных данных.

Будьте аккуратны при использовании `GLOBAL`. За дополнительной информацией обращайтесь в раздел [Распределенные подзапросы](../../../sql-reference/operators/in.md#select-distributed-subqueries).

## Неявные преобразования типов {#implicit-type-conversion}

Запросы `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` и `FULL JOIN` поддерживают неявные преобразования типов для ключей соединения. Однако запрос не может быть выполнен, если не существует типа, к которому можно привести значения ключей с обеих сторон (например, нет типа, который бы одновременно вмещал в себя значения `UInt64` и `Int64`, или `String` и `Int32`).

**Пример**

Рассмотрим таблицу `t_1`:
```text
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```
и таблицу `t_2`:
```text
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
вернёт результат:
```text
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

## Рекомендации по использованию {#usage-recommendations}

### Обработка пустых ячеек и NULL {#processing-of-empty-or-null-cells}

При соединении таблиц могут появляться пустые ячейки. Настройка [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) определяет, как ClickHouse заполняет эти ячейки.

Если ключами `JOIN` выступают поля типа [Nullable](../../../sql-reference/data-types/nullable.md), то строки, где хотя бы один из ключей имеет значение [NULL](../../../sql-reference/syntax.md#null-literal), не соединяются.

### Синтаксис {#syntax}

Требуется, чтобы столбцы, указанные в `USING`, назывались одинаково в обоих подзапросах, а остальные столбцы - по-разному. Изменить имена столбцов в подзапросах можно с помощью синонимов.

В секции `USING` указывается один или несколько столбцов для соединения, что обозначает условие на равенство этих столбцов. Список столбцов задаётся без скобок. Более сложные условия соединения не поддерживаются.

### Ограничения cинтаксиса {#syntax-limitations}

Для множественных секций `JOIN` в одном запросе `SELECT`:

-   Получение всех столбцов через `*` возможно только при объединении таблиц, но не подзапросов.
-   Секция `PREWHERE` недоступна.

Для секций `ON`, `WHERE` и `GROUP BY`:

-   Нельзя использовать произвольные выражения в секциях `ON`, `WHERE`, и `GROUP BY`, однако можно определить выражение в секции `SELECT` и затем использовать его через алиас в других секциях.

### Производительность {#performance}

При запуске `JOIN`, отсутствует оптимизация порядка выполнения по отношению к другим стадиям запроса. Соединение (поиск в «правой» таблице) выполняется до фильтрации в `WHERE` и до агрегации. Чтобы явно задать порядок вычислений, рекомендуется выполнять `JOIN` подзапроса с подзапросом.

Каждый раз для выполнения запроса с одинаковым `JOIN`, подзапрос выполняется заново — результат не кэшируется. Это можно избежать, используя специальный движок таблиц [Join](../../../engines/table-engines/special/join.md), представляющий собой подготовленное множество для соединения, которое всегда находится в оперативке.

В некоторых случаях это более эффективно использовать [IN](../../operators/in.md) вместо `JOIN`.

Если `JOIN` необходим для соединения с таблицами измерений (dimension tables - сравнительно небольшие таблицы, которые содержат свойства измерений - например, имена для рекламных кампаний), то использование `JOIN` может быть не очень удобным из-за громоздкости синтаксиса, а также из-за того, что правая таблица читается заново при каждом запросе. Специально для таких случаев существует функциональность «Внешние словари», которую следует использовать вместо `JOIN`. Дополнительные сведения смотрите в разделе «Внешние словари».


### Ограничения по памяти {#memory-limitations}

По умолчанию ClickHouse использует алгоритм [hash join](https://ru.wikipedia.org/wiki/Алгоритм_соединения_хешированием). ClickHouse берет правую таблицу и создает для нее хеш-таблицу в оперативной памяти. При включённой настройке `join_algorithm = 'auto'`, после некоторого порога потребления памяти ClickHouse переходит к алгоритму [merge join](https://ru.wikipedia.org/wiki/Алгоритм_соединения_слиянием_сортированных_списков). Описание алгоритмов `JOIN` см. в настройке [join_algorithm](../../../operations/settings/settings.md#settings-join_algorithm).

Если вы хотите ограничить потребление памяти во время выполнения операции `JOIN`, используйте настройки:

-   [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — ограничивает количество строк в хеш-таблице.
-   [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — ограничивает размер хеш-таблицы.

По достижении любого из этих ограничений ClickHouse действует в соответствии с настройкой [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode).

## Примеры {#examples}

Пример:

``` sql
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

``` text
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
