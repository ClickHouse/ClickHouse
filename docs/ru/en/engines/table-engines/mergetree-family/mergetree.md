---
description: 'Движки таблиц семейства `MergeTree` рассчитаны на высокую скорость
  приёма и огромные объёмы данных.'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'Движок таблицы MergeTree'
doc_type: 'справочник'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # Движок таблицы MergeTree
</div>

Движок `MergeTree` и другие движки семейства `MergeTree` (например, `ReplacingMergeTree`, `AggregatingMergeTree`) — самые распространённые и самые надёжные движки таблиц в ClickHouse.

Движки таблиц семейства `MergeTree` рассчитаны на высокую скорость приёма данных и очень большие объёмы данных.
Операции вставки создают части таблицы, которые затем фоновый процесс объединяет с другими частями таблицы.

Основные возможности движков таблиц семейства `MergeTree`.

* Первичный ключ таблицы определяет порядок сортировки внутри каждой части таблицы (кластеризованный индекс). При этом первичный ключ указывает не на отдельные строки, а на блоки по 8192 строк, называемые гранулами. Благодаря этому первичные ключи даже для огромных наборов данных остаются достаточно компактными, чтобы помещаться в оперативной памяти, и при этом обеспечивают быстрый доступ к данным на диске.

* Таблицы можно разбивать на партиции с помощью произвольного выражения партиционирования. Отсечение партиций позволяет не читать партиции, когда это следует из запроса.

* Данные можно реплицировать между несколькими узлами кластера для высокой доступности, переключения при отказе и обновлений без простоя. См. [Репликация данных](/ru/engines/table-engines/mergetree-family/replication.md).

* Движки таблиц `MergeTree` поддерживают различные виды статистики и методы сэмплирования для оптимизации запросов.

:::note
Несмотря на похожее название, движок [Merge](/ru/engines/table-engines/special/merge) отличается от движков `*MergeTree`.
:::

<div id="table_engine-mergetree-creating-a-table">
  ## Создание таблиц
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

Подробное описание параметров см. в описании оператора [CREATE TABLE](/ru/sql-reference/statements/create/table.md)

<div id="mergetree-query-clauses">
  ### Предложения запроса
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — имя и параметры движка. `ENGINE = MergeTree()`. У движка `MergeTree` параметров нет.

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — ключ сортировки.

Кортеж из имен столбцов или произвольных выражений. Пример: `ORDER BY (CounterID + 1, EventDate)`.

Если первичный ключ не определен (то есть `PRIMARY KEY` не указан), ClickHouse использует ключ сортировки в качестве первичного ключа.

Если сортировка не требуется, можно использовать синтаксис `ORDER BY tuple()`.
Либо, если включен параметр `create_table_empty_primary_key_by_default`, в операторы `CREATE TABLE` неявно добавляется `ORDER BY ()`. См. раздел [Выбор первичного ключа](#selecting-a-primary-key).

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — [ключ партиционирования](/ru/engines/table-engines/mergetree-family/custom-partitioning-key.md). Необязателен. В большинстве случаев ключ партиционирования не нужен, а если партиционирование всё же требуется, то, как правило, достаточно партиционирования по месяцам. Партиционирование не ускоряет запросы (в отличие от выражения ORDER BY). Никогда не используйте слишком детализированное партиционирование. Не партиционируйте данные по идентификаторам или именам клиентов (вместо этого сделайте идентификатор или имя клиента первым столбцом в выражении ORDER BY).

Для партиционирования по месяцам используйте выражение `toYYYYMM(date_column)`, где `date_column` — столбец с датой типа [Date](/ru/sql-reference/data-types/date.md). Имена партиций в этом случае имеют формат `"YYYYMM"`.

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — первичный ключ, если он [отличается от ключа сортировки](#choosing-a-primary-key-that-differs-from-the-sorting-key). Необязательно.

Указание ключа сортировки (с помощью предложения `ORDER BY`) неявно задаёт первичный ключ.
Обычно нет необходимости указывать первичный ключ в дополнение к ключу сортировки.

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — выражение для семплирования. Необязательный параметр.

Если оно указано, оно должно входить в первичный ключ.
Выражение для семплирования должно возвращать беззнаковое целое число.

Пример: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

<div id="ttl">
  #### TTL
</div>

`TTL` — список правил, задающих срок хранения строк и логику автоматического перемещения частей [между дисками и томами](#table_engine-mergetree-multiple-volumes). Необязательно.

Выражение должно возвращать `Date` или `DateTime`, например `TTL date + INTERVAL 1 DAY`.

Тип правила `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` задаёт действие, которое должно быть выполнено с частью, если условие выражения выполняется (достигается текущее время): удаление устаревших строк, перемещение части (если выражение выполняется для всех строк в части) на указанный диск (`TO DISK 'xxx'`) или в указанный том (`TO VOLUME 'xxx'`), либо агрегация значений в устаревших строках. Тип правила по умолчанию — удаление (`DELETE`). Можно задать список из нескольких правил, но правило `DELETE` должно быть только одно.

Подробнее см. в разделе [TTL для столбцов и таблиц](#table_engine-mergetree-ttl)

<div id="settings">
  #### НАСТРОЙКИ
</div>

См. [настройки MergeTree](../../../operations/settings/merge-tree-settings.md).

**Пример настройки Sections**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

В этом примере мы задаём партицирование по месяцам.

Мы также задаём выражение для семплирования в виде хеша по ID пользователя. Это позволяет псевдослучайно распределить данные в таблице для каждого `CounterID` и `EventDate`. Если при выборе данных указать предложение [SAMPLE](/ru/sql-reference/statements/select/sample), ClickHouse вернёт равномерную псевдослучайную выборку данных для подмножества пользователей.

Настройку `index_granularity` можно опустить, поскольку 8192 — значение по умолчанию.

<details markdown="1">
  <summary>Устаревший метод создания таблицы</summary>

  :::note
  Не используйте этот метод в новых проектах. По возможности переведите старые проекты на метод, описанный выше.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **Параметры MergeTree()**

  * `date-column` — Имя столбца типа [Date](/ru/sql-reference/data-types/date.md). ClickHouse автоматически создаёт партиции по месяцам на основе этого столбца. Имена партиций имеют формат `"YYYYMM"`.
  * `sampling_expression` — Выражение для семплирования.
  * `(primary, key)` — Первичный ключ. Тип: [Tuple()](/ru/sql-reference/data-types/tuple.md)
  * `index_granularity` — Гранулярность индекса. Число строк данных между &quot;метками&quot; индекса. Значение 8192 подходит для большинства задач.

  **Пример**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  Движок `MergeTree` настраивается так же, как в примере выше для основного способа настройки движка.
</details>

<div id="mergetree-data-storage">
  ## Хранение данных
</div>

Таблица состоит из частей данных, отсортированных по первичному ключу.

При вставке данных в таблицу создаются отдельные части данных, и каждая из них лексикографически сортируется по первичному ключу. Например, если первичный ключ — `(CounterID, Date)`, данные в части сортируются по `CounterID`, а внутри каждого `CounterID` — по `Date`.

Данные, относящиеся к разным партициям, разделяются на разные части. В фоновом режиме ClickHouse выполняет слияние частей данных для более эффективного хранения. Части, относящиеся к разным партициям, не сливаются. Механизм слияния не гарантирует, что все строки с одинаковым первичным ключом будут находиться в одной и той же части данных.

Части данных могут храниться в формате `Wide` или `Compact`. В формате `Wide` каждый столбец хранится в отдельном файле в файловой системе, а в формате `Compact` все столбцы хранятся в одном файле. Формат `Compact` можно использовать для повышения производительности небольших и частых вставок.

Формат хранения данных определяется настройками `min_bytes_for_wide_part` и `min_rows_for_wide_part` движка таблицы. Если количество байт или строк в части данных меньше значения соответствующей настройки, часть хранится в формате `Compact`. В противном случае она хранится в формате `Wide`. Если ни одна из этих настроек не задана, части данных хранятся в формате `Wide`.

Каждая часть данных логически разделена на гранулы. Гранула — это наименьший неделимый набор данных, который ClickHouse считывает при выборке данных. ClickHouse не разделяет строки или значения, поэтому каждая гранула всегда содержит целое число строк. Первая строка гранулы помечается значением первичного ключа этой строки. Для каждой части данных ClickHouse создаёт файл индекса, в котором хранятся метки. Для каждого столбца, независимо от того, входит он в первичный ключ или нет, ClickHouse также хранит те же метки. Эти метки позволяют находить данные непосредственно в файлах столбцов.

Размер гранулы ограничивается настройками `index_granularity` и `index_granularity_bytes` движка таблицы. Количество строк в грануле находится в диапазоне `[1, index_granularity]` и зависит от размера строк. Размер гранулы может превышать `index_granularity_bytes`, если размер одной строки больше значения этой настройки. В этом случае размер гранулы равен размеру строки.

<div id="primary-keys-and-indexes-in-queries">
  ## Первичные ключи и индексы в запросах
</div>

Возьмём в качестве примера первичный ключ `(CounterID, Date)`. В этом случае сортировку и индекс можно представить следующим образом:

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

Если запрос к данным содержит:

* `CounterID in ('a', 'h')`, сервер читает данные в диапазонах отметок `[0, 3)` и `[6, 8)`.
* `CounterID IN ('a', 'h') AND Date = 3`, сервер читает данные в диапазонах отметок `[1, 3)` и `[7, 8)`.
* `Date = 3`, сервер читает данные в диапазоне отметок `[1, 10]`.

Приведённые выше примеры показывают, что использовать индекс всегда эффективнее, чем выполнять полное сканирование.

Разреженный индекс допускает чтение дополнительных данных. При чтении одного диапазона первичного ключа из каждого блока данных может быть прочитано до `index_granularity * 2` дополнительных строк.

Разреженные индексы позволяют работать с очень большим числом строк таблицы, потому что в большинстве случаев такие индексы помещаются в оперативную память.

ClickHouse не требует уникального первичного ключа. Вы можете вставлять несколько строк с одним и тем же первичным ключом.

Вы можете использовать выражения типа `Nullable` в предложениях `PRIMARY KEY` и `ORDER BY`, но делать это настоятельно не рекомендуется. Чтобы разрешить эту возможность, включите настройку [allow&#95;nullable&#95;key](/ru/operations/settings/merge-tree-settings/#allow_nullable_key). Для значений `NULL` в предложении `ORDER BY` применяется принцип [NULLS&#95;LAST](/ru/sql-reference/statements/select/order-by.md/#sorting-of-special-values).

<div id="selecting-a-primary-key">
  ### Выбор первичного ключа
</div>

Количество столбцов в первичном ключе явно не ограничено. В зависимости от структуры данных в первичный ключ можно включить больше или меньше столбцов. Это может:

* Повысить эффективность индекса.

  Если первичный ключ — `(a, b)`, то добавление ещё одного столбца `c` повысит производительность, если выполняются следующие условия:

  * Есть запросы с условием по столбцу `c`.
  * Часто встречаются длинные диапазоны данных (в несколько раз длиннее, чем `index_granularity`) с одинаковыми значениями `(a, b)`. Иными словами, добавление ещё одного столбца позволяет пропускать достаточно длинные диапазоны данных.

* Улучшить сжатие данных.

  ClickHouse сортирует данные по первичному ключу, поэтому чем более однородны данные, тем лучше сжатие.

* Обеспечить дополнительную логику при слиянии частей данных в движках [CollapsingMergeTree](/ru/engines/table-engines/mergetree-family/collapsingmergetree) и [SummingMergeTree](/ru/engines/table-engines/mergetree-family/summingmergetree.md).

  В этом случае имеет смысл указать *ключ сортировки*, отличный от первичного ключа.

Длинный первичный ключ отрицательно влияет на производительность вставки и расход памяти, но дополнительные столбцы в первичном ключе не влияют на производительность ClickHouse при выполнении запросов `SELECT`.

Вы можете создать таблицу без первичного ключа, используя синтаксис `ORDER BY tuple()`. В этом случае ClickHouse хранит данные в порядке вставки. Если вы хотите сохранить порядок данных при вставке с помощью запросов `INSERT ... SELECT`, установите [max&#95;insert&#95;threads = 1](/ru/operations/settings/settings#max_insert_threads).

Чтобы выбирать данные в исходном порядке, используйте [однопоточные](/ru/operations/settings/settings.md/#max_threads) запросы `SELECT`.

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### Выбор первичного ключа, отличающегося от ключа сортировки
</div>

Можно указать первичный ключ (выражение со значениями, которые записываются в файл индекса для каждой метки), отличный от ключа сортировки (выражения для сортировки строк в частях данных). В этом случае кортеж выражения первичного ключа должен быть префиксом кортежа выражения ключа сортировки.

Эта возможность полезна при использовании движков таблиц [SummingMergeTree](/ru/engines/table-engines/mergetree-family/summingmergetree.md) и
[AggregatingMergeTree](/ru/engines/table-engines/mergetree-family/aggregatingmergetree.md). В типичном случае при использовании этих движков таблица содержит два типа столбцов: *измерения* и *меры*. Типичные запросы агрегируют значения столбцов мер с произвольным `GROUP BY` и фильтрацией по измерениям. Поскольку SummingMergeTree и AggregatingMergeTree агрегируют строки с одинаковым значением ключа сортировки, в него естественно включить все измерения. В результате выражение ключа состоит из длинного списка столбцов, и этот список приходится часто обновлять при добавлении новых измерений.

В этом случае имеет смысл оставить в первичном ключе только несколько столбцов, которые обеспечат эффективное сканирование диапазонов, а остальные столбцы измерений добавить в кортеж ключа сортировки.

[ALTER](/ru/sql-reference/statements/alter/index.md) ключа сортировки — легковесная операция, потому что, когда новый столбец одновременно добавляется в таблицу и в ключ сортировки, существующие части данных не нужно изменять. Поскольку старый ключ сортировки является префиксом нового ключа сортировки и в только что добавленном столбце нет данных, в момент изменения таблицы данные отсортированы как по старому, так и по новому ключу сортировки.

<div id="use-of-indexes-and-partitions-in-queries">
  ### Использование индексов и партиций в запросах
</div>

Для запросов `SELECT` ClickHouse анализирует, можно ли использовать индекс. Индекс может использоваться, если условие `WHERE/PREWHERE` содержит выражение (либо целиком, либо как один из элементов конъюнкции), представляющее собой операцию сравнения на равенство или неравенство, либо если в нём есть `IN` или `LIKE` с фиксированным префиксом для столбцов или выражений, входящих в первичный ключ или ключ партиционирования, а также для некоторых частично повторяющихся функций этих столбцов или логических связей этих выражений.

Таким образом, можно быстро выполнять запросы по одному или нескольким диапазонам первичного ключа. В этом примере запросы будут выполняться быстро для конкретного тега отслеживания, для конкретного тега и диапазона дат, для конкретного тега и даты, для нескольких тегов с диапазоном дат и так далее.

Давайте рассмотрим движок, настроенный следующим образом:

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

В этом случае в запросах:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse будет использовать индекс первичного ключа, чтобы отсечь неподходящие данные, и месячный ключ партиционирования, чтобы исключить партиции, выходящие за пределы нужных диапазонов дат.

Приведённые выше запросы показывают, что индекс используется даже для сложных выражений. Чтение из таблицы организовано так, что использование индекса не может быть медленнее полного сканирования.

В примере ниже индекс использовать нельзя.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Чтобы проверить, может ли ClickHouse использовать индекс при выполнении запроса, используйте настройки [force&#95;index&#95;by&#95;date](/ru/operations/settings/settings.md/#force_index_by_date) и [force&#95;primary&#95;key](/ru/operations/settings/settings#force_primary_key).

Ключ партиционирования по месяцам позволяет читать только те блоки данных, которые содержат даты из нужного диапазона. В этом случае блок данных может содержать данные за множество дат (вплоть до целого месяца). Внутри блока данные сортируются по первичному ключу, и дата может не быть в нём первым столбцом. Поэтому запрос только с условием по дате, не задающим префикс первичного ключа, приведёт к чтению большего объёма данных, чем запрос для одной даты.

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### Использование индекса для детерминированных выражений в первичных ключах
</div>

Первичный ключ может содержать не только имена столбцов, но и выражения. Эти выражения не ограничиваются простыми цепочками функций: это могут быть произвольные деревья выражений (например, вложенные функции и составные выражения), если они детерминированы.

Выражение является **детерминированным**, если оно всегда возвращает один и тот же результат для одних и тех же входных значений (например: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`; в отличие от `now()` или `rand()`). Если первичный ключ содержит детерминированные выражения, ClickHouse может применять их к константным значениям из запроса и использовать результат для построения условий по индексу первичного ключа. Это позволяет пропускать данные для таких предикатов, как `=`, `IN` и `has`.

Распространённый сценарий использования — сделать первичный ключ компактным (например, хранить хеш вместо длинного `String`), при этом сохранив возможность использовать индекс в предикатах по исходному столбцу.

Пример детерминированного (но не инъективного) первичного ключа:

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

Примеры предикатов, для которых может использоваться индекс:

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

В этих случаях ClickHouse вычисляет `length('alice')` (и другие константы) один раз и использует значения длины, чтобы сузить диапазоны в индексе первичного ключа. Поскольку длина строки **не является инъективной**, разные строки `user_id` могут иметь одинаковую длину, поэтому индекс может читать дополнительные гранулы (ложноположительные срабатывания). Результат при этом остаётся корректным, потому что исходный предикат (`user_id = ...`, `IN` и т. д.) по-прежнему применяется после чтения.

Если детерминированное выражение также является **инъективным** (разные входные значения не могут давать один и тот же результат для используемых типов аргументов), то ClickHouse дополнительно может эффективно использовать индекс для отрицательных форм: `!=`, `NOT IN` и `NOT has(...)`. Например, `reverse(p)` и `hex(p)` являются инъективными для `String`.

Пример инъективного первичного ключа:

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

Поддерживаются и более сложные инъективные выражения, например:

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

Примеры предикатов, для которых можно использовать индекс:

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### Использование индекса для частично монотонных первичных ключей
</div>

Рассмотрим, например, дни месяца. В пределах одного месяца они образуют [монотонную последовательность](https://en.wikipedia.org/wiki/Monotonic_function), но на более длительных интервалах монотонность нарушается. Это частично монотонная последовательность. Если пользователь создает таблицу с частично монотонным первичным ключом, ClickHouse, как обычно, создает разреженный индекс. Когда пользователь выбирает данные из таблицы такого типа, ClickHouse анализирует условия запроса. Если пользователь хочет получить данные между двумя метками индекса и обе эти метки находятся в пределах одного месяца, ClickHouse может использовать индекс в этом конкретном случае, поскольку может вычислить расстояние между параметрами запроса и метками индекса.

ClickHouse не может использовать индекс, если значения первичного ключа в диапазоне параметров запроса не образуют монотонную последовательность. В этом случае ClickHouse использует метод полного сканирования.

ClickHouse применяет эту логику не только к последовательностям дней месяца, но и к любому первичному ключу, который представляет собой частично монотонную последовательность.

<div id="table_engine-mergetree-data_skipping-indexes">
  ### Индексы пропуска данных
</div>

Объявление индекса находится в разделе столбцов запроса `CREATE`.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

Для таблиц семейства `*MergeTree` можно задавать индексы пропуска данных.

Эти индексы агрегируют некоторую информацию о заданном выражении по блокам, состоящим из `granularity_value` гранул (размер гранулы задаётся с помощью настройки `index_granularity` в движке таблицы). Затем эти агрегаты используются в `SELECT`-запросах, чтобы уменьшить объём данных, считываемых с диска, пропуская большие блоки данных, для которых условие запроса `where` не может быть выполнено.

Предложение `GRANULARITY` можно опустить; значение `granularity_value` по умолчанию равно 1.

**Пример**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Индексы из примера ClickHouse может использовать для уменьшения объема данных, считываемых с диска, в следующих запросах:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

Индексы пропуска данных также можно создавать для составных столбцов:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### Типы индексов пропуска данных
</div>

Движок таблицы `MergeTree` поддерживает следующие типы индексов пропуска данных.
Подробнее о том, как индексы пропуска данных используются для оптимизации производительности,
см. в разделе [&quot;Понимание индексов пропуска данных в ClickHouse&quot;](/ru/optimize/skipping-indexes).

* индекс [`MinMax`](#minmax)
* индекс [`Set`](#set)
* индекс [`bloom_filter`](#bloom-filter)
* индекс [`ngrambf_v1`](#n-gram-bloom-filter) *(Устарел)*
* индекс [`tokenbf_v1`](#token-bloom-filter) *(Устарел)*
* индекс [`text`](#text)
* индекс [`vector_similarity`](#vector-similarity)

<div id="minmax">
  #### Индекс пропуска данных MinMax
</div>

Для каждой гранулы индекса сохраняются минимальное и максимальное значения выражения.
(Если выражение имеет тип `tuple`, для каждого элемента кортежа сохраняются его минимальное и максимальное значения.)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

Для каждой гранулы индекса хранится не более `max_rows` уникальных значений указанного выражения.
`max_rows = 0` означает &quot;хранить все уникальные значения&quot;.

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### bloom-фильтр
</div>

Для каждой гранулы индекса хранится [bloom-фильтр](https://en.wikipedia.org/wiki/Bloom_filter) для указанных столбцов.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

Параметр `false_positive_rate` может принимать значение от 0 до 1 (по умолчанию — `0.025`) и задаёт вероятность ложноположительного срабатывания (что увеличивает объём читаемых данных).

Поддерживаются следующие типы данных:

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Тип данных Map: создание индекса по ключам или значениям
Для типа данных `Map` клиент может указать, должен ли индекс создаваться по ключам или по значениям, с помощью функций [`mapKeys`](/ru/sql-reference/functions/tuple-map-functions.md/#mapKeys) или [`mapValues`](/ru/sql-reference/functions/tuple-map-functions.md/#mapValues).
:::

:::note Тип данных JSON: индексация JSON-путей
Для типа данных [`JSON`](/ru/sql-reference/data-types/newjson) можно создать индекс bloom-фильтр по набору путей с помощью функции [`JSONAllPaths`](/ru/sql-reference/functions/json-functions#JSONAllPaths). Это позволяет пропускать гранулы, в которых отсутствует запрашиваемый JSON-путь. Подробности см. в разделе [Индексы пропуска данных для JSON](/ru/sql-reference/data-types/newjson#data-skipping-indexes-for-json).
:::

<div id="n-gram-bloom-filter">
  #### N-граммный bloom-фильтр *(Устарело)*
</div>

:::note
Начиная с версии ClickHouse 26.2, когда индекс `text` стал общедоступным (GA), индекс `ngrambf_v1` больше не рекомендуется использовать для полнотекстового поиска.

Подробнее см. на странице [&quot;Полнотекстовый поиск с помощью текстовых индексов&quot;](./textindexes.md).
:::

Для каждой гранулы индекса хранится [bloom-фильтр](https://en.wikipedia.org/wiki/Bloom_filter) для [n-грамм](https://en.wikipedia.org/wiki/N-gram) указанных столбцов.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| Параметр                        | Описание                                                                                                                            |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `n`                             | размер n-граммы                                                                                                                     |
| `size_of_bloom_filter_in_bytes` | Размер bloom-фильтра в байтах. Здесь можно использовать большое значение, например `256` или `512`, поскольку оно хорошо сжимается. |
| `number_of_hash_functions`      | Количество хеш-функций, используемых в bloom-фильтре.                                                                               |
| `random_seed`                   | seed для хеш-функций bloom-фильтра.                                                                                                 |

Этот индекс работает только со следующими типами данных:

* [`String`](/ru/sql-reference/data-types/string.md)
* [`FixedString`](/ru/sql-reference/data-types/fixedstring.md)
* [`Map`](/ru/sql-reference/data-types/map.md)

Чтобы оценить параметры `ngrambf_v1`, можно использовать следующие [Пользовательские функции (UDF)](/ru/sql-reference/statements/create/function.md).

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

Чтобы использовать эти функции, необходимо указать как минимум два параметра:

* `total_number_of_all_grams`
* `probability_of_false_positives`

Например, в грануле содержится `4300` n-грамм, и вы ожидаете, что число ложноположительных срабатываний будет меньше `0.0001`.
Остальные параметры затем можно оценить, выполнив следующие запросы:

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

Разумеется, вы также можете использовать эти функции, чтобы оценить параметры для других условий.
Приведённые выше функции основаны на калькуляторе bloom-фильтра [здесь](https://hur.st/bloomfilter).

<div id="token-bloom-filter">
  #### Токенный bloom-фильтр
</div>

:::note
Начиная с выхода индекса `text` в GA в ClickHouse версии 26.2 индекс `tokenbf_v1` больше не рекомендуется для полнотекстового поиска.

Подробнее см. на странице [&quot;Полнотекстовый поиск с текстовыми индексами&quot;](./textindexes.md).
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### Разреженный фильтр Блума для sparse grams
</div>

Разреженный фильтр Блума для sparse grams похож на `ngrambf_v1`, но использует [токены sparse grams](/ru/sql-reference/functions/string-functions.md/#sparseGrams) вместо n-грамм.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### Текстовый индекс
</div>

Создает инвертированный индекс по токенизированным строковым данным, обеспечивая эффективный и предсказуемый полнотекстовый поиск. Подробнее см. [здесь](textindexes.md).

<div id="vector-similarity">
  #### Векторное сходство
</div>

Поддерживается приблизительный поиск ближайших соседей; подробности см. [здесь](annindexes.md).

<div id="functions-support">
  ### Поддержка функций
</div>

Условия в предложении `WHERE` содержат вызовы функций, работающих со столбцами. Если столбец входит в индекс, ClickHouse пытается использовать этот индекс при вычислении функций. ClickHouse поддерживает разные подмножества функций для работы с индексами.

Индексы типа `set` могут использоваться всеми функциями. Остальные типы индексов поддерживаются следующим образом:

| Функция (оператор) / индекс                                                                                               | первичный ключ | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | text |
| ------------------------------------------------------------------------------------------------------------------------- | -------------- | ------ | -------------- | -------------- | ---------------- | ---------------- | ---- |
| [equals (=, ==)](/ru/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notEquals(!=, &lt;&gt;)](/ru/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [like](/ru/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [notLike](/ru/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✗    |
| [match](/ru/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗              | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [startsWith](/ru/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔              | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [endsWith](/ru/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗              | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [multiSearchAny](/ru/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗              | ✗      | ✔              | ✗              | ✗                | ✗                | ✔    |
| [multiSearchAnyUTF8](/ru/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [multiMatchAny](/ru/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [in](/ru/sql-reference/functions/in-functions)                                                                               | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notIn](/ru/sql-reference/functions/in-functions)                                                                            | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [less (`<`)](/ru/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greater (`>`)](/ru/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [lessOrEquals (`<=`)](/ru/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greaterOrEquals (`>=`)](/ru/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [empty](/ru/sql-reference/functions/array-functions/#empty)                                                                  | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [notEmpty](/ru/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗              | ✔      | ✗              | ✗              | ✗                | ✔                | ✗    |
| [has](/ru/sql-reference/functions/array-functions#has)                                                                       | ✔              | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [hasAny](/ru/sql-reference/functions/array-functions#hasAny)                                                                 | ✗              | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasAll](/ru/sql-reference/functions/array-functions#hasAll)                                                                 | ✗              | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasToken](/ru/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenOrNull](/ru/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenCaseInsensitive (`*`)](/ru/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/ru/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗              | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasAnyTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [hasAllTokens](/ru/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [pointInPolygon](/ru/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔              | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [mapContains (mapContainsKey)](/ru/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsKeyLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValue](/ru/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValueLike](/ru/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗              | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |

Функции с константным аргументом, который меньше размера n-граммы, не могут использоваться `ngrambf_v1` для оптимизации запросов.

(*) Чтобы `hasTokenCaseInsensitive` и `hasTokenCaseInsensitiveOrNull` работали эффективно, индекс `tokenbf_v1` должен быть создан по данным, приведённым к нижнему регистру, например `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
Bloom-фильтры могут давать ложноположительные срабатывания, поэтому индексы `ngrambf_v1`, `tokenbf_v1`, `sparse_grams` и `bloom_filter` нельзя использовать для оптимизации запросов, где результат функции должен быть `false`.

Например:

* Могут быть оптимизированы:
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* Не могут быть оптимизированы:
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## Проекции
</div>

Проекции похожи на [materialized views](/ru/sql-reference/statements/create/view), но определяются на уровне частей данных. Они обеспечивают согласованность и автоматически используются в запросах.

:::note
При использовании проекций также следует учитывать настройку [force&#95;optimize&#95;projection](/ru/operations/settings/settings#force_optimize_projection).
:::

Проекции не поддерживаются в командах `SELECT` с модификатором [FINAL](/ru/sql-reference/statements/select/from#final-modifier).

<div id="projection-query">
  ### Запрос проекции
</div>

Запрос проекции определяет проекцию. Он неявно выбирает данные из родительской таблицы.
**Синтаксис**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

Проекции можно изменять или удалять с помощью оператора [ALTER](/ru/sql-reference/statements/alter/projection.md).

<div id="projection-index">
  ### Индексы проекций
</div>

Индексы проекций расширяют подсистему проекций, предоставляя легковесный и явный способ задавать индексы на уровне проекций.
Внешне индекс проекции по-прежнему остаётся проекцией, но с упрощённым синтаксисом и более ясным назначением: он задаёт выражение, предназначенное для фильтрации, а не для хранения материализованных данных.
Внутренне индекс проекции, в отличие от обычной проекции, не материализует исходную таблицу в переставленном порядке строк.
Вместо этого перестановка хранится в виде числового столбца `_part_offset`, то есть `SELECT _part_offset ORDER BY <index_expr>`.

<div id="projection-index-syntax">
  #### Синтаксис
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

Пример:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### Типы индексов
</div>

В настоящее время поддерживаются:

* **basic**: эквивалентен обычному индексу MergeTree для выражения.

В будущем этот механизм позволит добавлять и другие типы индексов.

<div id="projection-storage">
  ### Хранение проекций
</div>

Проекции хранятся внутри каталога части. Они похожи на индекс, но содержат подкаталог, в котором хранится часть анонимной таблицы `MergeTree`. Эта таблица создаётся на основе запроса, определяющего проекцию. Если указан `GROUP BY`, нижележащий движок хранения становится [AggregatingMergeTree](aggregatingmergetree.md), а все агрегатные функции преобразуются в `AggregateFunction`. Если указан `ORDER BY`, таблица `MergeTree` использует его как выражение первичного ключа. В процессе слияния часть проекции сливается с использованием процедуры слияния своего хранилища. Контрольная сумма части родительской таблицы объединяется с контрольной суммой части проекции. Остальные задачи обслуживания аналогичны индексам пропуска.

<div id="projection-query-analysis">
  ### Анализ запроса
</div>

1. Проверьте, можно ли использовать проекцию для выполнения данного запроса, то есть дает ли она тот же результат, что и запрос к базовой таблице.
2. Выберите наилучший из допустимых вариантов, содержащий минимальное количество гранул для чтения.
3. Конвейер выполнения запроса, использующий проекции, будет отличаться от того, который использует исходные части. Если в некоторых частях проекция отсутствует, можно добавить конвейер, чтобы построить ее на лету.

<div id="concurrent-data-access">
  ## Одновременный доступ к данным
</div>

Для одновременного доступа к таблице используется многоверсионность. Иными словами, когда таблица одновременно читается и обновляется, данные считываются из набора частей, актуального на момент выполнения запроса. Длительные блокировки отсутствуют. Операции вставки не мешают операциям чтения.

Чтение из таблицы автоматически выполняется параллельно.

<div id="table_engine-mergetree-ttl">
  ## TTL для столбцов и таблиц
</div>

Определяет срок хранения значений.

Предложение `TTL` можно задать для всей таблицы и для каждого отдельного столбца. `TTL` на уровне таблицы также может задавать логику автоматического перемещения данных между дисками и томами, а также повторного сжатия частей, для которых срок хранения всех данных уже истёк.

Выражения должны иметь тип данных [Date](/ru/sql-reference/data-types/date.md), [Date32](/ru/sql-reference/data-types/date32.md), [DateTime](/ru/sql-reference/data-types/datetime.md) или [DateTime64](/ru/sql-reference/data-types/datetime64.md).

:::tip[Избегайте недетерминированных функций в выражениях TTL]
TTL вычисляется во время фоновых слияний, а не во время вставки.
Такие функции, как `rand()`, `now()` или `now64()`, будут вычисляться заново при каждом слиянии, что приведёт к непредсказуемому поведению при удалении.
ClickHouse блокирует выражения, которые вообще не зависят от столбцов, но в настоящее время не отклоняет недетерминированные функции в сочетании со ссылкой на столбец (например, `ts + rand()`). Для предсказуемых результатов выражения TTL должны основываться исключительно на детерминированных значениях, вычисляемых из столбцов.
:::

**Синтаксис**

Задание time-to-live для столбца:

```sql
TTL time_column
TTL time_column + interval
```

Чтобы задать `interval`, используйте операторы [интервалов времени](/ru/sql-reference/operators#operators-for-working-with-dates-and-times), например:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### TTL для столбца
</div>

Когда срок хранения значений в столбце истекает, ClickHouse заменяет их значениями по умолчанию для типа данных этого столбца. Если срок хранения всех значений столбца в части данных истёк, ClickHouse удаляет этот столбец из части данных в файловой системе.

Предложение `TTL` нельзя использовать для столбцов ключа.

**Примеры**

<div id="creating-a-table-with-ttl">
  #### Создание таблицы с `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### Добавление TTL к столбцу в существующей таблице
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### Изменение TTL для столбца
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### TTL таблицы
</div>

Для таблицы можно задать выражение для удаления устаревших строк, а также несколько выражений для автоматического перемещения частей между [дисками или томами](#table_engine-mergetree-multiple-volumes). Когда строки в таблице устаревают, ClickHouse удаляет все соответствующие строки. Для перемещения или повторного сжатия частей все строки в части должны удовлетворять условиям выражения `TTL`.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

Тип правила TTL может следовать за каждым выражением TTL. Он определяет действие, которое должно быть выполнено, когда условие выражения выполнено (достигнуто текущее время):

* `DELETE` - удалить устаревшие строки (действие по умолчанию);
* `RECOMPRESS codec_name` - повторно сжать часть данных с помощью `codec_name`;
* `TO DISK 'aaa'` - переместить часть на диск `aaa`;
* `TO VOLUME 'bbb'` - переместить часть на том `bbb`;
* `GROUP BY` - агрегировать устаревшие строки.

Действие `DELETE` можно использовать вместе с `WHERE` clause, чтобы удалять только часть устаревших строк на основе условия фильтрации:

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY`-выражение должно быть префиксом первичного ключа таблицы.

Если столбец не входит в `GROUP BY`-выражение и не задан явно в операторе `SET`, то в результирующей строке он содержит произвольное значение из сгруппированных строк (как если бы к нему была применена агрегатная функция `any`).

**Примеры**

<div id="creating-a-table-with-ttl">
  #### Создание таблицы с `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### Изменение `TTL` у таблицы:
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

Создание таблицы, в которой срок хранения строк составляет один месяц. Просроченные строки, даты которых приходятся на понедельник, удаляются:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### Создание таблицы, в которой истёкшие строки пересжимаются:
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

Создание таблицы, в которой строки с истекшим TTL агрегируются. В результирующих строках `x` содержит максимальное значение среди сгруппированных строк, `y` — минимальное, а `d` — любое значение из сгруппированных строк.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### Удаление устаревших данных
</div>

Данные с истёкшим `TTL` удаляются, когда ClickHouse выполняет слияние частей данных.

Когда ClickHouse обнаруживает, что срок `TTL` данных истёк, он выполняет внеплановое слияние. Чтобы управлять частотой таких слияний, можно задать `merge_with_ttl_timeout`. Если значение слишком мало, будет выполняться много внеплановых слияний, которые могут потреблять значительные ресурсы.

Если выполнить запрос `SELECT` в промежутке между слияниями, можно получить устаревшие данные. Чтобы этого избежать, перед `SELECT` используйте запрос [OPTIMIZE](/ru/sql-reference/statements/optimize.md).

**См. также**

* настройка [ttl&#95;only&#95;drop&#95;parts](/ru/operations/settings/merge-tree-settings#ttl_only_drop_parts)

<div id="disk-types">
  ## Типы дисков
</div>

Помимо локальных блочных устройств, ClickHouse поддерживает следующие типы хранилища:

* [`s3` для S3 и MinIO](#table_engine-mergetree-s3)
* [`gcs` для GCS](/ru/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` для Azure Blob Storage](/ru/operations/storing-data#azure-blob-storage)
* [`hdfs` для HDFS](/ru/engines/table-engines/integrations/hdfs)
* [`web` для доступа из веб-источников в режиме только для чтения](/ru/operations/storing-data#web-storage)
* [`cache` для локального кэширования](/ru/operations/storing-data#using-local-cache)
* [`s3_plain` для резервных копий в S3](/ru/operations/backup/disk)
* [`s3_plain_rewritable` для неизменяемых таблиц без репликации в S3](/ru/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## Использование нескольких блочных устройств для хранения данных
</div>

<div id="introduction">
  ### Введение
</div>

Движки таблиц семейства `MergeTree` могут хранить данные на нескольких блочных устройствах. Например, это полезно, когда данные определённой таблицы естественным образом делятся на «горячие» и «холодные». Самые свежие данные запрашиваются регулярно, но требуют лишь небольшого объёма места. Напротив, объёмные исторические данные запрашиваются редко. Если доступно несколько дисков, «горячие» данные можно размещать на быстрых дисках (например, NVMe SSD или в памяти), а «холодные» — на относительно медленных (например, HDD).

Это относится ко всем типам дисков, включая S3 и другие диски объектного хранилища. Например, можно распределить данные по нескольким S3 бакетам в рамках одного тома или создать многоуровневые политики, которые перемещают данные с локальных дисков в S3. Подробнее см. в разделе [Using S3 disks with multiple volumes](#s3-multiple-volumes).

Часть данных — минимальная единица перемещения для таблиц семейства `MergeTree`. Данные, относящиеся к одной части, хранятся на одном диске. Части данных могут перемещаться между дисками в фоновом режиме (в соответствии с пользовательскими настройками), а также с помощью запросов [ALTER](/ru/sql-reference/statements/alter/partition).

<div id="terms">
  ### Термины
</div>

* Диск — блочное устройство, смонтированное в файловой системе.
* Диск по умолчанию — диск, на котором хранится путь, указанный в настройке сервера [path](/ru/operations/server-configuration-parameters/settings.md/#path).
* Том — упорядоченный набор одинаковых дисков (аналог [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
* Политика хранения — набор томов и правил перемещения данных между ними.

Имена, заданные описанным объектам, можно найти в системных таблицах [system.storage&#95;policies](/ru/operations/system-tables/storage_policies) и [system.disks](/ru/operations/system-tables/disks). Чтобы применить к таблице одну из настроенных политик хранения, используйте настройку `storage_policy` для таблиц семейства движков `MergeTree`.

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### Конфигурация
</div>

Диски, тома и политики хранения следует объявлять внутри тега `<storage_configuration>` в файле в каталоге `config.d`.

:::tip
Диски также можно объявлять в разделе `SETTINGS` запроса. Это полезно
для разового анализа, чтобы временно подключить диск, который, например, доступен по URL.
Подробнее см. в разделе [динамическое хранилище](/ru/operations/storing-data#dynamic-configuration).
:::

Структура конфигурации:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Теги:

* `<disk_name_N>` — имя диска. Имена всех дисков должны различаться.
* `path` — путь, по которому сервер хранит данные (папки `data` и `shadow`); должен оканчиваться на &#39;/&#39;.
* `keep_free_space_bytes` — объём свободного места на диске, который должен быть зарезервирован.

Порядок определения дисков не имеет значения.

Разметка конфигурации политик хранения:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Теги:

* `policy_name_N` — Имя политики. Имена политик должны быть уникальными.
* `volume_name_N` — Имя тома. Имена томов должны быть уникальными.
* `disk` — диск внутри тома.
* `max_data_part_size_bytes` — максимальный размер части, которая может храниться на любом из дисков тома. Если предполагаемый размер слитой части превышает `max_data_part_size_bytes`, эта часть будет записана на следующий том. По сути, эта возможность позволяет хранить новые/небольшие части на горячем томе (SSD) и перемещать их на холодный том (HDD), когда они достигают большого размера. Не используйте эту настройку, если в вашей политике только один том.
* `move_factor` — когда объем доступного пространства становится меньше этого коэффициента, данные автоматически начинают перемещаться на следующий том, если он есть (по умолчанию 0.1). ClickHouse сортирует существующие части по размеру от большей к меньшей (в порядке убывания) и выбирает части, суммарный размер которых достаточен для выполнения условия `move_factor`. Если суммарного размера всех частей недостаточно, будут перемещены все части.
* `perform_ttl_move_on_insert` — Отключает TTL move при INSERT части данных. По умолчанию (если включено), если выполняется вставка части данных, которая уже подпадает под правило TTL move, она сразу записывается на том/диск, указанный в правиле перемещения. Это может значительно замедлить вставку, если том/диск пункта назначения медленный (например, S3). Если параметр отключен, уже просроченная часть данных записывается на том по умолчанию, а затем сразу перемещается на TTL-том.
* `load_balancing` - Политика балансировки дисков: `round_robin` или `least_used`.
* `least_used_ttl_ms` - Настройка тайм-аута (в миллисекундах) для обновления доступного пространства на всех дисках (`0` — обновлять всегда, `-1` — никогда не обновлять, значение по умолчанию — `60000`). Обратите внимание: если диск может использоваться только ClickHouse и не подвержен онлайн-расширению/сжатию файловой системы, можно использовать `-1`; во всех остальных случаях это не рекомендуется, поскольку со временем это приведет к некорректному распределению пространства.
* `prefer_not_to_merge` — Вам не следует использовать эту настройку. Отключает слияние частей данных на этом томе (это вредно и приводит к снижению производительности). Когда эта настройка включена (не делайте этого), слияние данных на этом томе не допускается (а это плохо). Это позволяет (но вам это не нужно) управлять (если вы хотите чем-то управлять, вы совершаете ошибку) тем, как ClickHouse работает с медленными дисками (но ClickHouse знает лучше, поэтому, пожалуйста, не используйте эту настройку).
* `volume_priority` — Определяет приоритет (порядок), в котором заполняются тома. Меньшее значение означает более высокий приоритет. Значения параметра должны быть натуральными числами и в совокупности покрывать диапазон от 1 до N (где N — наименьший приоритет) без пропуска каких-либо чисел.
  * Если помечены *все* тома, они получают приоритет в заданном порядке.
  * Если помечены только *некоторые* тома, то тома без метки получают самый низкий приоритет и упорядочиваются в том порядке, в котором они определены в config.
  * Если не помечен *ни один* том, его приоритет задается в соответствии с порядком объявления в конфигурации.
  * Два тома не могут иметь одинаковое значение приоритета.

Примеры конфигурации:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

В данном примере политика `hdd_in_order` реализует подход [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling). Таким образом, эта политика определяет только один том (`single`), а части данных хранятся на всех его дисках по кругу. Такая политика может быть весьма полезной, если в системе смонтировано несколько одинаковых дисков, но RAID не настроен. Имейте в виду, что каждый отдельный диск сам по себе ненадёжен, и это может потребовать компенсации за счёт фактора репликации 3 или более.

Если в системе доступны разные типы дисков, вместо этого можно использовать политику `moving_from_ssd_to_hdd`. Том `hot` состоит из SSD-диска (`fast_ssd`), а максимальный размер части, которую можно хранить на этом томе, составляет 1GB. Все части размером более 1GB будут храниться непосредственно на томе `cold`, который содержит HDD-диск `disk1`.
Кроме того, как только диск `fast_ssd` заполнится более чем на 80%, данные будут перенесены на `disk1` фоновым процессом.

Порядок перечисления томов в рамках политики хранения важен, если хотя бы у одного из перечисленных томов явно не задан параметр `volume_priority`.
Как только том переполняется, данные перемещаются на следующий. Порядок перечисления дисков также важен, поскольку данные записываются на них по очереди.

При создании таблицы к ней можно применить одну из настроенных политик хранения:

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

Политика хранения `default` предполагает использование только одного тома, состоящего из одного диска, указанного в `<path>`.
После создания таблицы вы можете изменить политику хранения с помощью запроса [ALTER TABLE ... MODIFY SETTING], при этом новая политика должна включать все старые диски и тома с теми же именами.

Количество потоков, выполняющих фоновые перемещения частей данных, можно изменить с помощью настройки [background&#95;move&#95;pool&#95;size](/ru/operations/server-configuration-parameters/settings.md/#background_move_pool_size).

<div id="details">
  ### Подробности
</div>

В случае таблиц `MergeTree` данные записываются на диск разными способами:

* В результате вставки (запрос `INSERT`).
* Во время фоновых слияний и [мутаций](/ru/sql-reference/statements/alter#mutations).
* При загрузке с другой реплики.
* В результате замораживания партиции [ALTER TABLE ... FREEZE PARTITION](/ru/sql-reference/statements/alter/partition#freeze-partition).

Во всех этих случаях, кроме мутаций и замораживания партиций, часть сохраняется на томе и диске в соответствии с заданной политикой хранения:

1. Выбирается первый том (в порядке объявления), на котором достаточно места для хранения части (`unreserved_space > current_part_size`) и который допускает хранение частей такого размера (`max_data_part_size_bytes > current_part_size`).
2. Внутри этого тома выбирается диск, следующий за тем, который использовался для хранения предыдущего фрагмента данных, и на котором свободного места больше, чем размер части (`unreserved_space - keep_free_space_bytes > current_part_size`).

Внутри системы мутации и замораживание партиций используют [hard links](https://en.wikipedia.org/wiki/Hard_link). Жесткие ссылки между разными дисками не поддерживаются, поэтому в таких случаях итоговые части сохраняются на тех же дисках, что и исходные.

В фоновом режиме части перемещаются между томами на основе объема свободного места (параметр `move_factor`) в соответствии с порядком, в котором тома объявлены в файле конфигурации.
Данные никогда не переносятся с последнего тома и на первый том. Для мониторинга фоновых перемещений можно использовать системные таблицы [system.part&#95;log](/ru/operations/system-tables/part_log) (поле `type = MOVE_PART`) и [system.parts](/ru/operations/system-tables/parts.md) (поля `path` и `disk`). Кроме того, подробную информацию можно найти в серверном журнале.

Пользователь может принудительно переместить часть или партицию с одного тома на другой с помощью запроса [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/ru/sql-reference/statements/alter/partition), при этом учитываются все ограничения фоновых операций. Запрос сам инициирует перемещение и не ждет завершения фоновых операций. Пользователь получит сообщение об ошибке, если свободного места недостаточно или если не выполнено какое-либо из необходимых условий.

Перемещение данных не мешает репликации. Поэтому для одной и той же таблицы на разных репликах можно задавать разные политики хранения.

После завершения фоновых слияний и мутаций старые части удаляются только спустя некоторое время (`old_parts_lifetime`).
В течение этого времени они не перемещаются на другие тома или диски. Поэтому, пока части не будут окончательно удалены, они по-прежнему учитываются при оценке занятого места на диске.

Пользователь может сбалансированно распределять новые крупные части по разным дискам тома [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) с помощью настройки [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/ru/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod).

<div id="table_engine-mergetree-s3">
  ## Использование внешнего хранилища для данных
</div>

Движки таблиц семейства [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree.md) могут хранить данные в `S3`, `AzureBlobStorage` и `HDFS`, используя диски типов `s3`, `azure_blob_storage` и `hdfs` соответственно. Подробнее см. в разделе [настройка параметров внешнего хранилища](/ru/operations/storing-data.md/#configuring-external-storage).

Пример использования [S3](https://aws.amazon.com/s3/) в качестве внешнего хранилища с диском типа `s3`.

Конфигурационная разметка:

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

См. также [настройку параметров внешнего хранилища](/ru/operations/storing-data.md/#configuring-external-storage).

<div id="s3-multiple-volumes">
  ### Использование S3-дисков с несколькими томами
</div>

S3-диски (и другие диски Объектного хранилища) можно использовать в политиках хранения с несколькими дисками и томами так же, как и локальные диски. Это позволяет распределять данные по нескольким S3 бакетам в рамках одного тома (по типу JBOD) или настраивать политики многоуровневого хранения с S3-томами.

Например, чтобы распределять данные между двумя S3 бакетами по схеме round-robin:

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

Вы также можете объединять локальные и S3-тома в многоуровневой политике хранения, например перемещая данные с локального SSD в S3 по мере их старения:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
При использовании `use_environment_credentials` для аутентификации в S3 учетные данные окружения (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) являются общими для всех S3-дисков. Использовать разные учетные данные окружения для разных дисков невозможно. Если для каждого S3-диска нужны свои учетные данные, используйте вместо этого явные настройки `access_key_id` и `secret_access_key` для каждого диска.
:::

Можно настроить нереплицируемые таблицы семейства MergeTree по схеме «один пишущий узел, много читающих» в общем хранилище. Это обеспечивается автоматическим обновлением списка частей, которое можно настроить на читающих узлах. Обратите внимание, что для этого нужны общие метаданные файловой системы для всех реплик (или `table_disk = true` с локальным для таблицы диском). См. [refresh&#95;parts&#95;interval and table&#95;disk](/ru/operations/storing-data.md/#refresh-parts-interval-and-table-disk).

:::note конфигурация кэша
В версиях ClickHouse с 22.3 по 22.7 используется другая конфигурация кэша; если вы используете одну из этих версий, см. [using local cache](/ru/operations/storing-data.md/#using-local-cache).
:::

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_part` — Имя части.
* `_part_index` — Последовательный индекс части в результате запроса.
* `_part_starting_offset` — Накопительный номер начальной строки части в результате запроса.
* `_part_offset` — Номер строки в части.
* `_part_granule_offset` — Номер гранулы в части.
* `_partition_id` — Имя партиции.
* `_part_uuid` — Уникальный идентификатор части (если включена настройка MergeTree `assign_part_uuids`).
* `_part_data_version` — Версия данных части (либо минимальный номер блока, либо версия мутации).
* `_partition_value` — Значения (кортеж) выражения `partition by`.
* `_sample_factor` — Коэффициент выборки (из запроса).
* `_block_number` — Исходный номер блока для строки, присвоенный при вставке; сохраняется при слияниях, если включена настройка `enable_block_number_column`.
* `_block_offset` — Исходный номер строки в блоке, присвоенный при вставке; сохраняется при слияниях, если включена настройка `enable_block_offset_column`.
* `_disk_name` — Имя диска, используемого для хранения данных.

<div id="column-statistics">
  ## Статистика столбцов
</div>

<CloudNotSupportedBadge />

Объявление статистики задаётся в разделе столбцов запроса `CREATE` для таблиц семейства `*MergeTree*`:

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

Статистикой также можно управлять с помощью команд `ALTER`:

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

Эта легковесная статистика собирает сводную информацию о распределении значений в столбцах. Она хранится в каждой части и обновляется при каждой вставке.
Её можно использовать для оптимизации PREWHERE, только если включить `set use_statistics = 1`.

<div id="part-pruning-with-statistics">
  #### Отсечение частей с помощью статистики
</div>

Когда включён параметр `use_statistics_for_part_pruning`, статистику можно использовать для отсечения частей.
В настоящее время отсечение частей поддерживают только статистики `MinMax` и `Basic`. Когда такая статистика определена для столбца, ClickHouse отслеживает минимальное и максимальное значения этого столбца в каждой части.
Отсечение частей позволяет пропускать чтение целых частей данных, когда условие фильтра запроса не может соответствовать ни одной строке в этой части.

**Пример:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### Доступные типы статистики столбцов
</div>

* `Basic`

  Компактный набор сводных характеристик по столбцу, каждая из которых представлена одним значением. В зависимости от типа столбца заполняются следующие данные:

  * для любого столбца, значения которого представлены числами (целые числа, числа с плавающей точкой, `Decimal*`, `Date*`, `DateTime*`, `Enum*`, `IPv4`, ...): минимальное и максимальное значение, что позволяет оценивать селективность фильтров диапазона и выполнять отсечение частей;
  * для столбцов `String` и `FixedString`: суммарная длина в байтах всех значений, отличных от `NULL` (на её основе можно вычислить среднюю длину строк);
  * для столбцов `Nullable` и `LowCardinality(Nullable)`: количество значений `NULL`, которое оптимизатор использует, чтобы исключать строки с `NULL` из оценок селективности.

    Одна статистика `Basic` может одновременно заполнять несколько таких характеристик — например, для столбца `Nullable(UInt32)` она отслеживает и числовые min/max, и количество значений `NULL`. По сравнению с `MinMax`, `Basic` дополнительно работает со столбцами `String` / `FixedString`, а также может быть объявлена для обёрток `Nullable` над типами вроде `UUID` или `IPv6` исключительно для отслеживания количества значений `NULL`.

    Синтаксис: `basic`

* `MinMax`

  Минимальное и максимальное значение столбца, что позволяет оценивать селективность фильтров диапазона для числовых столбцов.

  Синтаксис: `minmax`

* `TDigest`

:::warning
Статистика типа `tdigest` требует значительных затрат на создание и может замедлить приём данных.
:::

[TDigest](https://github.com/tdunning/t-digest) — скетчи, позволяющие вычислять приближённые перцентили (например, 90-й перцентиль) для числовых столбцов.

Синтаксис: `tdigest`

* `Uniq`

  [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) — скетчи, позволяющие оценить, сколько различных значений содержит столбец.

  Синтаксис: `uniq`

* `CountMin`

:::warning
Статистика типа `countmin` требует значительных затрат на создание и может замедлить приём данных.
:::

[CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) — скетчи, дающие приблизительную оценку частоты каждого значения в столбце.

Синтаксис `countmin`

<div id="supported-data-types">
  ### Поддерживаемые типы данных
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, булевый, Enum* | IPv4 | String or FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

Все перечисленное выше также поддерживает обёртки `Nullable` и `LowCardinality(Nullable)` для указанных типов. `Basic` также можно объявлять для `Nullable`-обёрток типов вроде `UUID` или `IPv6` исключительно для подсчёта числа значений NULL.

<div id="supported-operations">
  ### Поддерживаемые операции
</div>

|          | Фильтры по равенству (==) | Фильтры диапазона (`>, >=, <, <=`) |
| -------- | ------------------------- | ---------------------------------- |
| Basic    | ✗                         | ✔ (только для числовых столбцов)   |
| CountMin | ✔                         | ✗                                  |
| MinMax   | ✗                         | ✔ (только для числовых столбцов)   |
| TDigest  | ✗                         | ✔ (только для числовых столбцов)   |
| Uniq     | ✔                         | ✗                                  |

Для `Basic` на столбцах `String` / `FixedString` статистика фиксирует только суммарную
длину в байтах для значений non-NULL (используется для оценки средней длины строки) и количество NULL;
для фильтров диапазона и отсечения частей она не используется.

<div id="column-level-settings">
  ## Настройки на уровне столбца
</div>

Некоторые настройки MergeTree можно переопределять на уровне столбца:

* `max_compress_block_size` — Максимальный размер блоков несжатых данных перед их сжатием при записи в таблицу.
* `min_compress_block_size` — Минимальный размер блоков несжатых данных, необходимый для сжатия при записи следующей отметки.

Пример:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

Настройки на уровне столбца можно изменять или удалять с помощью [ALTER MODIFY COLUMN](/ru/sql-reference/statements/alter/column.md), например:

* Удалите `SETTINGS` из объявления столбца:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* Изменить настройку:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* Сбросьте одну или несколько настроек; при этом также удаляется указание настройки в выражении столбца в запросе CREATE таблицы.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```