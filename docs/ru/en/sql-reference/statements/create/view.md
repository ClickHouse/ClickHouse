---
description: 'Документация по команде CREATE VIEW'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

Создает новое представление. Представления могут быть [обычными](#normal-view), [материализованными](#materialized-view), [обновляемыми материализованными](#refreshable-materialized-view) и [оконными](/ru/sql-reference/statements/create/view#window-view).

<div id="normal-view">
  ## Обычное представление
</div>

Синтаксис:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

Обычные представления не хранят никаких данных. При каждом обращении они просто читают данные из другой таблицы. Иными словами, обычное представление — это не более чем сохранённый запрос. При чтении из представления этот сохранённый запрос используется в предложении [FROM](../../../sql-reference/statements/select/from.md) как подзапрос.

Например, предположим, что вы создали представление:

```sql
CREATE VIEW view AS SELECT ...
```

и написали запрос:

```sql
SELECT a, b, c FROM view
```

Этот запрос полностью эквивалентен варианту с подзапросом:

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## Параметризованное представление
</div>

Параметризованные представления похожи на обычные представления, но могут создаваться с параметрами, значения которых подставляются не сразу. Эти представления можно использовать с табличными функциями: в качестве имени функции указывается имя представления, а в качестве аргументов — значения параметров.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

Приведённый выше код создаёт представление над таблицей, которое можно использовать как табличную функцию, подставляя параметры, как показано ниже.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## Materialized View
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` и `IF NOT EXISTS` являются взаимоисключающими: их нельзя использовать вместе, иначе возникнет синтаксическая ошибка.

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

`CREATE OR REPLACE MATERIALIZED VIEW` атомарно заменяет существующее materialized view и связанную с ним внутреннюю таблицу хранения (если она есть). Для этой операции требуется движок базы данных `Atomic` или `Replicated`.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

Основные особенности:

* **Без предложения `TO`**: старая внутренняя таблица удаляется и создаётся новая. Существующие данные во внутренней таблице будут потеряны, если не указан `POPULATE`.
* **С предложением `TO`**: заменяется только определение представления; целевая таблица и данные в ней не затрагиваются.
* Совместимо с `REFRESH`, `ON CLUSTER` и всеми параметрами движка. `POPULATE` поддерживается только для баз данных `Atomic` — для баз данных `Replicated` он отклоняется (см. примечание о `POPULATE` ниже).
* Требуются привилегии `CREATE VIEW` и `DROP VIEW`.

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` поддерживается только для движков баз данных `Atomic` и `Replicated`. Для движка базы данных `Ordinary` эта команда не поддерживается.
:::

**Примеры:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
Ниже приведено пошаговое руководство по использованию [materialized views](/ru/guides/developer/cascading-materialized-views.md).
:::

Materialized views хранят данные, преобразованные соответствующим запросом [SELECT](../../../sql-reference/statements/select/index.md).

При создании materialized view без `TO [db].[table]` необходимо указать `ENGINE` — движок таблицы для хранения данных.

При создании materialized view с `TO [db].[table]` нельзя одновременно использовать `POPULATE`.

Materialized view работает следующим образом: при вставке данных в таблицу, указанную в `SELECT`, часть вставленных данных преобразуется этим запросом `SELECT`, а результат вставляется в представление.

:::note
Materialized views в ClickHouse при вставке в целевую таблицу используют **имена столбцов**, а не порядок столбцов. Если каких-либо имен столбцов нет в результате запроса `SELECT`, ClickHouse использует значение по умолчанию, даже если столбец не является [Nullable](../../data-types/nullable.md). Надежная практика — задавать псевдонимы для каждого столбца при использовании Materialized views.

Materialized views в ClickHouse реализованы скорее как insert triggers. Если в запросе представления есть агрегация, она применяется только к батчу только что вставленных данных. Любые изменения существующих данных исходной таблицы (например, update, delete, drop partition и т. д.) не изменяют materialized view.

Materialized views в ClickHouse не имеют детерминированного поведения в случае ошибок. Это означает, что блоки, которые уже были записаны, сохранятся в целевой таблице, а все блоки после ошибки — нет.

По умолчанию, если отправка в одно из представлений генерирует исключение, запрос `INSERT` завершается ошибкой. При этом не гарантируется, что к этому моменту блок уже достиг исходной таблицы — это зависит от момента в конвейере вставки, а не от ошибки представления. Повторите неудавшийся `INSERT` с дедупликацией вставок (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`), чтобы обеспечить доставку exactly-once в исходную таблицу и все зависимые представления.

Параметр `materialized_views_ignore_errors=true` в запросе `INSERT` меняет только способ обработки ошибок: каждая ошибка представления записывается как предупреждение, а запрос `INSERT` завершается успешно. Доставка в пункт назначения проблемного представления выполняется частично — блоки, обработанные до исключения, сохраняются, а сбойный блок и все последующие блоки для этого представления отбрасываются. Представления ниже по цепочке от этого пункта назначения видят только те блоки, которые были доставлены, поэтому доставка в них тоже будет частичной. Параллельные представления (и их нисходящие цепочки), в которых не было исключения, заполняются полностью, а запись в исходную таблицу выполняется как обычно. Поскольку `INSERT` сообщает об успехе, клиент не получает сигнала о сбое и автоматический повтор не запускается; используйте этот параметр только в тех случаях, когда запись в исходную таблицу не должна блокироваться из-за проблем на стороне представления (например, для таблиц `system.*_log`).

`materialized_views_ignore_errors` по умолчанию имеет значение `true` для таблиц `system.*_log`.
:::

Если указать `POPULATE`, существующие данные таблицы будут вставлены в представление при его создании, как при выполнении `CREATE TABLE ... AS SELECT ...`. В противном случае запрос будет содержать только данные, вставленные в таблицу после создания представления. Мы **не рекомендуем** использовать `POPULATE`, поскольку данные, вставленные в таблицу во время создания представления, в него не попадут.

:::note
Поскольку `POPULATE` работает как `CREATE TABLE ... AS SELECT ...`, у него есть ограничения:

* Не поддерживается с базой данных Replicated
* Не поддерживается в ClickHouse Cloud

Вместо этого можно использовать отдельный `INSERT ... SELECT`.
:::

Запрос `SELECT` может содержать `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Обратите внимание, что соответствующие преобразования выполняются независимо для каждого блока вставляемых данных. Например, если задан `GROUP BY`, данные агрегируются во время вставки, но только в пределах одного пакета вставляемых данных. Далее данные дополнительно не агрегируются. Исключение — использование `ENGINE`, который сам выполняет агрегацию данных, например `SummingMergeTree`.

Если materialized view использует конструкцию `TO [db.]name`, можно выполнить `DETACH` представления, запустить `ALTER` для целевой таблицы, а затем выполнить `ATTACH` ранее отсоединённого (`DETACH`) представления.

Обратите внимание, что на materialized view влияет настройка [optimize&#95;on&#95;insert](/ru/operations/settings/settings#optimize_on_insert). Данные сливаются перед вставкой в представление.

Представления выглядят так же, как обычные таблицы. Например, они перечисляются в результате запроса `SHOW TABLES`.

Чтобы удалить представление, используйте [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Хотя `DROP TABLE` тоже работает для представлений.

<div id="sql_security">
  ## безопасность SQL
</div>

`DEFINER` и `SQL SECURITY` позволяют указать, от имени какого пользователя ClickHouse выполнять базовый запрос представления.
`SQL SECURITY` имеет три допустимых значения: `DEFINER`, `INVOKER` или `NONE`. В предложении `DEFINER` можно указать любого существующего пользователя или `CURRENT_USER`.

Следующая таблица поясняет, какие права и какому пользователю нужны, чтобы выполнять `SELECT` из представления.
Обратите внимание: независимо от параметра безопасности SQL, в любом случае для чтения из представления по-прежнему требуется `GRANT SELECT ON <view>`.

| Параметр безопасности SQL | Представление                                                                 | materialized view                                                                                                           |
| ------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DEFINER alice`           | У `alice` должен быть grant `SELECT` для исходной таблицы представления.      | У `alice` должен быть grant `SELECT` для исходной таблицы представления и grant `INSERT` для целевой таблицы представления. |
| `INVOKER`                 | У пользователя должен быть grant `SELECT` для исходной таблицы представления. | Для materialized view нельзя указать `SQL SECURITY INVOKER`.                                                                |
| `NONE`                    | -                                                                             | -                                                                                                                           |

:::note
`SQL SECURITY NONE` — устаревший параметр. Любой пользователь с правами на создание представлений с `SQL SECURITY NONE` сможет выполнять произвольные запросы.
Поэтому для создания представления с этим параметром требуется `GRANT ALLOW SQL SECURITY NONE TO <user>`.
:::

Если `DEFINER`/`SQL SECURITY` не указаны, используются значения по умолчанию:

* `SQL SECURITY`: `INVOKER` для обычных представлений и `DEFINER` для materialized view ([настраивается через settings](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER`: `CURRENT_USER` ([настраивается через settings](../../../operations/settings/settings.md#default_view_definer))

Если представление attach без указания `DEFINER`/`SQL SECURITY`, значением по умолчанию будет `SQL SECURITY NONE` для materialized view и `SQL SECURITY INVOKER` для обычного представления.

Чтобы изменить безопасность SQL для существующего представления, используйте

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### Примеры
</div>

```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

Эта возможность устарела и в будущем будет удалена.

Для удобства старая документация доступна [здесь](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

<div id="refreshable-materialized-view">
  ## Refreshable Materialized View
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

где `interval` — последовательность простых интервалов:

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

Клауза `REFRESH` должна содержать как минимум одно из `EVERY`, `AFTER` или `DEPENDS ON`. Просто `REFRESH` (без них) не допускается. `REFRESH DEPENDS ON ...` без `EVERY`/`AFTER` — это сокращённая запись `REFRESH AFTER 0 SECOND DEPENDS ON ...`; см. [Зависимости обновления](#refresh-dependencies) ниже.

Периодически выполняет соответствующий запрос и сохраняет его результат в таблице.

* Если указан `APPEND`, при каждом обновлении в таблицу добавляются строки без удаления существующих. Вставка не является атомарной — так же, как в обычном запросе `INSERT INTO ... SELECT`.
* В противном случае при каждом обновлении предыдущее содержимое таблицы атомарно заменяется.

Отличия от обычных, не refreshable materialized view:

* Нет insert trigger. Когда новые данные вставляются в таблицу, указанную в `SELECT`, они *не* передаются автоматически в refreshable materialized view. Вместо этого вставка данных происходит только во время периодических или ручных обновлений.
* На запрос `SELECT` не накладываются ограничения. Допускаются табличные функции (например, `url()`), views, UNION, JOIN.

:::note
Параметры в части запроса `REFRESH ... SETTINGS` — это настройки обновления (например, `refresh_retries`), а не обычные настройки (например, `max_threads`). Обычные настройки можно задать с помощью `SETTINGS` в конце запроса.
:::

<div id="refresh-schedule">
  ### Расписание обновления
</div>

Примеры расписаний обновления:

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` случайным образом изменяет время каждого обновления, например:

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

Для данного представления одновременно может выполняться не более одного обновления. Например, если обновление представления с `REFRESH EVERY 1 MINUTE` занимает 2 минуты, оно просто будет обновляться каждые 2 минуты. Если затем оно ускорится и начнет обновляться за 10 секунд, то снова вернется к обновлению раз в минуту. (В частности, оно не будет обновляться каждые 10 секунд, чтобы наверстать пропущенные обновления, — никакой очереди пропущенных обновлений здесь не существует.)

Обычно первое обновление запускается сразу после создания materialized view: время с момента последнего обновления считается бесконечным, поэтому по любому расписанию обновление нужно выполнить немедленно. Если указан `EMPTY`, это начальное обновление пропускается, и первое обновление произойдет в следующий запланированный момент; например, для `EVERY 1 HOUR` первое обновление произойдет в конце текущего часа.

<div id="in-replicated-db">
  ### В базе данных Replicated
</div>

Если refreshable materialized view находится в [базе данных Replicated](../../../engines/database-engines/replicated.md), реплики координируют работу между собой так, что в каждый запланированный момент обновление выполняет только одна реплика. Требуется движок таблицы [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md), чтобы все реплики видели данные, полученные в результате обновления.

В режиме `APPEND` координацию можно отключить с помощью `SETTINGS all_replicas = 1`. В этом случае реплики выполняют обновление независимо друг от друга. Тогда ReplicatedMergeTree не требуется.

В режиме без `APPEND` поддерживается только координируемое обновление. Для нескоординированного обновления используйте базу данных `Atomic` и запрос `CREATE ... ON CLUSTER`, чтобы создать refreshable materialized view на всех репликах.

Координация выполняется через Keeper. Путь к znode определяется настройкой сервера [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path).

<div id="refresh-dependencies">
  ### Зависимости при обновлении
</div>

`DEPENDS ON` синхронизирует обновление разных таблиц:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

Обновление зависимого представления начнется только после того, как завершатся обновления всех представлений, от которых оно зависит.

Чтобы запустить обновление сразу после обновления другого представления:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

Или, что эквивалентно:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` работает только между refreshable materialized view. В частности, если зависимое представление использует `TO <table>`, обязательно указывайте имя представления, а не таблицы. Если список `DEPENDS ON` содержит обычную таблицу, представление, не являющееся refreshable materialized view, или опечатку, представление никогда не будет обновляться и будет иметь состояние `MissingDependencies` в `system.view_refreshes`. Зависимости можно изменить или удалить с помощью `ALTER`, см. [Изменение параметров обновления](#changing-refresh-parameters).
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### Использование DEPENDS ON для согласованной задержки распространения данных
</div>

Если оба представления используют `REFRESH EVERY` с одинаковым периодом, зависимость действует в каждом временном интервале.

Например, предположим, что представления X и Y используют `REFRESH EVERY 1 HOUR`, а Y читает из выходной таблицы X. Без зависимостей Y обычно будет видеть данные X, полученные при обновлении за предыдущий час. С `DEPENDS ON X` обновление Y в 11:00 начнется только после завершения обновления X в 11:00.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

И зависимость, и зависящий от неё объект могут независимо пропускать временные интервалы, если обновления выполняются дольше, чем период обновления. Нет гарантии, что зависящий объект будет обновляться ровно один раз на каждое обновление зависимости.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### Использование DEPENDS ON для потоковой обработки батчами
</div>

Если `REFRESH EVERY` не используется, зависимое представление X обновляется, если все его зависимости обновились хотя бы один раз с момента последнего обновления X. `REFRESH AFTER T` добавляет задержку: зависимое представление начнет обновляться через T после того, как зависимость завершит обновление.

Циклические зависимости допустимы и полезны. Рассмотрим следующий граф refreshable materialized views:

1. X берет батч строк из некоторого потока и помещает его в таблицу.
2. Затем Y и Z читают из этой таблицы, выполняют разную агрегацию и дописывают результаты в другие таблицы.
3. После полной обработки батча X берет следующий батч, и цикл повторяется.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

Полный пример:

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

Более длинные цепочки тоже работают.

Однако это хорошо работает только при включенной координации обновления, то есть когда представления находятся в базе данных Replicated или Shared. Без координации перезапуск сервера разрывает цикл, поэтому после каждого перезапуска нужно вручную выполнять `SYSTEM REFRESH VIEW`, а не только один раз после создания представлений.

<div id="refresh-settings">
  ### Настройки обновления
</div>

Доступны следующие настройки обновления:

* `refresh_retries` - Сколько раз повторять попытку, если запрос на обновление завершается с исключением. Если все повторные попытки окажутся неудачными, обновление будет пропущено до следующего запланированного времени. 0 означает отсутствие повторных попыток, -1 — бесконечное число повторных попыток. Значение по умолчанию: 2.
* `refresh_retry_initial_backoff_ms` - Задержка перед первой повторной попыткой, если `refresh_retries` не равно нулю. При каждой следующей повторной попытке задержка удваивается, максимум до `refresh_retry_max_backoff_ms`. Значение по умолчанию: 100 мс.
* `refresh_retry_max_backoff_ms` - Ограничение на экспоненциальный рост задержки между попытками обновления. Значение по умолчанию: 60000 мс (1 минута).
* `all_replicas` - В [базе данных Replicated](../../../engines/database-engines/replicated.md) с `APPEND` определяет, будут ли все реплики обновляться независимо или в каждый запланированный момент времени обновление будет выполнять только одна реплика. После создания представления изменить этот параметр нельзя. Значение по умолчанию: `false`.

<div id="changing-refresh-parameters">
  ### Изменение параметров обновления
</div>

Чтобы изменить параметры обновления существующего refreshable materialized view, используйте [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

Расписание (`EVERY` или `AFTER`) обязательно: этот оператор всегда заменяет *все* параметры обновления — расписание, `RANDOMIZE FOR`, `DEPENDS ON` и настройки обновления — на указанные значения. Всё, что не указано, сбрасывается до значения по умолчанию (для настроек) или удаляется (для зависимостей и случайного смещения).

:::note

* Чтобы изменить только настройки обновления (например, `refresh_retries`), повторно укажите текущее расписание:

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` не поддерживается для materialized view; необходимо использовать `MODIFY REFRESH`.

* Добавление или удаление `APPEND` не поддерживается.

* Настройку `all_replicas` нельзя изменить после создания.
  :::

Примеры:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### Другие операции
</div>

Состояние всех refreshable materialized views доступно в таблице [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). В частности, в ней содержатся ход обновления (если оно выполняется), время последнего и следующего обновления, а также текст исключения, если обновление завершилось ошибкой.

Чтобы вручную остановить, запустить, инициировать или отменить обновления, используйте [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views).

Чтобы дождаться завершения обновления, используйте [`SYSTEM WAIT VIEW`](../system.md#wait-view). Это особенно полезно, если нужно дождаться первоначального обновления после создания представления.

:::note
Интересный факт: запрос обновления может читать из представления, которое в этот момент обновляется, видя версию данных до обновления. Это означает, что вы можете реализовать игру «Жизнь» Конвея: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## Оконное представление
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Это экспериментальная возможность, которая в будущих выпусках может измениться с нарушением обратной совместимости. Чтобы включить использование оконных представлений и запроса `WATCH`, задайте настройку [allow&#95;experimental&#95;window&#95;view](/ru/operations/settings/settings#allow_experimental_window_view). Введите команду `set allow_experimental_window_view = 1`.
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

Оконное представление может агрегировать данные по временному окну и выводить результаты, когда окно готово выдать результат. Оно сохраняет частичные результаты агрегации во внутренней (или указанной) таблице, чтобы уменьшить задержку, и может записывать результат обработки в указанную таблицу или отправлять уведомления с помощью запроса WATCH.

Создание оконного представления похоже на создание `MATERIALIZED VIEW`. Оконному представлению требуется внутренний движок для хранения промежуточных данных. Внутреннее хранилище можно указать с помощью предложения `INNER ENGINE`; в противном случае оконное представление будет использовать `AggregatingMergeTree` в качестве внутреннего движка по умолчанию.

При создании оконного представления без `TO [db].[table]` необходимо указать `ENGINE` — движок таблицы для хранения данных.

<div id="time-window-functions">
  ### Функции временного окна
</div>

[Функции временного окна](../../functions/time-window-functions.md) используются для определения нижней и верхней границ окна для записей. Оконное представление необходимо использовать вместе с функцией временного окна.

<div id="time-attributes">
  ### ВРЕМЕННЫЕ АТРИБУТЫ
</div>

Оконное представление поддерживает обработку по **времени обработки** и **времени события**.

**Время обработки** позволяет оконному представлению формировать результаты на основе времени локальной машины и используется по умолчанию. Это наиболее простое понятие времени, но оно не обеспечивает детерминированности. Атрибут времени обработки можно задать, указав в качестве `time_attr` функции временного окна столбец таблицы или функцию `now()`. Следующий запрос создает оконное представление со временем обработки.

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**Время события** — это время, когда каждое отдельное событие произошло на устройстве-источнике. Обычно эта временная метка записывается в запись в момент её создания. Обработка по времени события позволяет получать согласованные результаты даже в случае событий, поступающих не по порядку, или запоздавших событий. Оконное представление поддерживает обработку по времени события с помощью синтаксиса `WATERMARK`.

Оконное представление предоставляет три стратегии водяных меток:

* `STRICTLY_ASCENDING`: Выдаёт водяную метку, равную максимальной наблюдаемой на данный момент временной метке. Строки, у которых временная метка меньше максимальной, не считаются запоздавшими.
* `ASCENDING`: Выдаёт водяную метку, равную максимальной наблюдаемой на данный момент временной метке минус 1. Строки, у которых временная метка равна максимальной или меньше неё, не считаются запоздавшими.
* `BOUNDED`: WATERMARK=INTERVAL. Выдаёт водяные метки, равные максимальной наблюдаемой временной метке минус указанная задержка.

Следующие запросы показывают примеры создания оконного представления с `WATERMARK`:

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

По умолчанию окно срабатывает при поступлении водяной метки, а элементы, поступившие позже неё, отбрасываются. Оконное представление поддерживает обработку поздних событий с помощью настройки `ALLOWED_LATENESS=INTERVAL`. Пример обработки опоздавших событий:

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

Обратите внимание, что элементы, выданные при позднем срабатывании, следует рассматривать как обновлённые результаты предыдущего вычисления. Вместо срабатывания в конце окна оконное представление сработает сразу при поступлении позднего события. Таким образом, для одного и того же окна будет получено несколько результатов. Пользователям нужно учитывать эти дублирующиеся результаты или выполнять их дедупликацию.

Вы можете изменить запрос `SELECT`, указанный в оконном представлении, с помощью оператора `ALTER TABLE ... MODIFY QUERY`. Структура данных, получающаяся в результате нового запроса `SELECT`, должна быть такой же, как у исходного запроса `SELECT`, как с предложением `TO [db.]name`, так и без него. Обратите внимание, что данные в текущем окне будут потеряны, поскольку промежуточное состояние нельзя использовать повторно.

<div id="monitoring-new-windows">
  ### Отслеживание новых окон
</div>

Оконное представление поддерживает запрос [WATCH](../../../sql-reference/statements/watch.md) для отслеживания изменений, либо можно использовать синтаксис `TO` для вывода результатов в таблицу.

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

Можно указать `LIMIT`, чтобы задать число обновлений, которые нужно получить до завершения запроса. Предложение `EVENTS` позволяет использовать краткую форму запроса `WATCH`: вместо результата запроса вы получите только последнюю водяную метку запроса.

<div id="settings-1">
  ### Настройки
</div>

* `window_view_clean_interval`: Интервал очистки оконного представления в секундах для удаления устаревших данных. Система сохраняет окна, которые еще не были полностью сгенерированы в соответствии с системным временем или конфигурацией `WATERMARK`, а остальные данные удаляются.
* `window_view_heartbeat_interval`: Интервал heartbeat в секундах, показывающий, что запрос watch активен.
* `wait_for_window_view_fire_signal_timeout`: Тайм-аут ожидания сигнала срабатывания оконного представления при обработке по времени события.

<div id="example">
  ### Пример
</div>

Предположим, нам нужно подсчитать количество журналов кликов за каждые 10 секунд в таблице журналов с именем `data`, структура которой такова:

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

Сначала создадим оконное представление с фиксированным окном и интервалом 10 секунд:

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Затем с помощью запроса `WATCH` получаем результаты.

```sql
WATCH wv
```

Когда журналы записываются в таблицу `data`,

```sql
INSERT INTO data VALUES(1,now())
```

Запрос `WATCH` должен вывести следующие результаты:

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

Также можно направить вывод в другую таблицу с помощью синтаксиса `TO`.

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Дополнительные примеры можно найти среди тестов ClickHouse с сохранением состояния (там они называются `*window_view*`).

<div id="window-view-usage">
  ### Использование оконного представления
</div>

Оконное представление полезно в следующих сценариях:

* **Мониторинг**: Агрегировать и вычислять метрики и журналы по времени, а результаты выводить в целевую таблицу. Панель мониторинга может использовать целевую таблицу как исходную таблицу.
* **Анализ**: Автоматически агрегировать данные и выполнять их предварительную обработку во временном окне. Это может быть полезно при анализе большого количества журналов. Предварительная обработка устраняет повторяющиеся вычисления в нескольких запросах и снижает задержку выполнения запросов.

<div id="related-content">
  ## Похожие материалы
</div>

* Блог: [Работа с данными временных рядов в ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* Блог: [Построение решения для обсервабилити с ClickHouse — Часть 2 — Трассировки](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## Временные представления
</div>

ClickHouse поддерживает **временные представления** со следующими характеристиками (где применимо — как и временные таблицы):

* **Время жизни сеанса**
  Временное представление существует только в рамках текущего сеанса. После завершения сеанса оно удаляется автоматически.

* **Без базы данных**
  Вы **не можете** указывать для временного представления имя базы данных. Оно существует вне баз данных (в пространстве имен сеанса).

* **Не реплицируется / без ON CLUSTER**
  Временные объекты локальны для сеанса и **не могут** создаваться с `ON CLUSTER`.

* **Разрешение имен**
  Если временный объект (таблица или представление) имеет то же имя, что и постоянный объект, и запрос ссылается на это имя **без** указания базы данных, используется **временный** объект.

* **Логический объект (без хранения данных)**
  Временное представление хранит только текст своего `SELECT` (внутри используется движок `View`). Оно не сохраняет данные и не поддерживает `INSERT`.

* **Предложение ENGINE**
  Указывать `ENGINE` **не** нужно; если задать `ENGINE = View`, оно будет проигнорировано / воспринято как то же логическое представление.

* **Безопасность / привилегии**
  Для создания временного представления требуется привилегия `CREATE TEMPORARY VIEW`, которая неявно выдается через `CREATE VIEW`.

* **SHOW CREATE**
  Используйте `SHOW CREATE TEMPORARY VIEW view_name;`, чтобы вывести DDL временного представления.

<div id="temporary-views-syntax">
  ### Синтаксис
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` **не** поддерживается для временных представлений (чтобы поведение соответствовало временным таблицам). Если вам нужно «заменить» временное представление, удалите его и создайте заново.

<div id="examples">
  ### Примеры
</div>

Создайте временную исходную таблицу и временное представление поверх неё:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

Показать DDL:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

Удалите его:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### Недопустимо / ограничения
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **не допускается** (используйте `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **не допускается**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **не допускается** (без указания базы данных).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **не допускается** (временные объекты локальны для сеанса).
* `POPULATE`, `REFRESH`, `TO [db.table]`, внутренние движки и все секции, специфичные для materialized view, → **не применимы** к временным представлениям.

<div id="temporary-views-distributed-notes">
  ### Примечания о распределённых запросах
</div>

Временное **представление** — это просто определение; передавать здесь нечего. Если ваше временное представление ссылается на временные **таблицы** (например, `Memory`), их данные могут передаваться на удалённые серверы при выполнении распределённого запроса так же, как и данные временных таблиц.

<div id="temporary-views-distributed-example">
  #### Пример
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```