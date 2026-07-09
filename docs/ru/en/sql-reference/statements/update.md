---
description: 'Легковесные обновления упрощают обновление данных в базе данных с помощью патч-частей.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: 'Оператор легковесного UPDATE'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
Легковесные обновления сейчас находятся в статусе бета.
Если у вас возникнут проблемы, пожалуйста, откройте issue в [репозитории ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

Оператор легковесного `UPDATE` обновляет строки в таблице `[db.]table`, соответствующие выражению `filter_expr`.
Он называется &quot;легковесным обновлением&quot;, чтобы отличать его от запроса [`ALTER TABLE ... UPDATE`](/ru/sql-reference/statements/alter/update), который представляет собой ресурсоёмкий процесс с перезаписью целых столбцов в частях данных.
Он доступен только для семейства движков таблиц [`MergeTree`](/ru/engines/table-engines/mergetree-family/mergetree).

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

`filter_expr` должен иметь тип `UInt8`. Этот запрос обновляет значения указанных столбцов значениями соответствующих выражений в строках, для которых `filter_expr` принимает ненулевое значение.
Значения приводятся к типу столбца с помощью оператора `CAST`. Обновление столбцов, используемых при вычислении основного ключа или ключа партиционирования, не поддерживается.

<div id="examples">
  ## Примеры
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## Легковесные обновления не обновляют данные немедленно
</div>

Легковесный `UPDATE` реализован с помощью **патч-частей** — особого типа частей данных, которые содержат только обновлённые столбцы и строки.
Легковесный `UPDATE` создаёт патч-части, но не вносит немедленных физических изменений в исходные данные в хранилище.
Процесс обновления похож на запрос `INSERT ... SELECT ...`, но запрос `UPDATE` ждёт, пока завершится создание патч-части, и только после этого возвращает результат.

Обновлённые значения:

* **Сразу видны** в запросах `SELECT` благодаря применению патчей
* **Физически материализуются** только во время последующих слияний и мутаций
* **Автоматически удаляются**, как только патчи будут материализованы во всех активных частях

<div id="lightweight-update-requirements">
  ## Требования к легковесным обновлениям
</div>

Легковесные обновления поддерживаются для движков [`MergeTree`](/ru/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/ru/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/ru/engines/table-engines/mergetree-family/collapsingmergetree), [`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree), а также для их вариантов [`Replicated`](/ru/engines/table-engines/mergetree-family/replication.md) и [`Shared`](/ru/cloud/reference/shared-merge-tree).

Чтобы использовать легковесные обновления, необходимо включить материализацию столбцов `_block_number` и `_block_offset` с помощью настроек таблицы [`enable_block_number_column`](/ru/operations/settings/merge-tree-settings#enable_block_number_column) и [`enable_block_offset_column`](/ru/operations/settings/merge-tree-settings#enable_block_offset_column).

<div id="lightweight-delete">
  ## Легковесные удаления
</div>

Запрос [легковесный `DELETE`](/ru/sql-reference/statements/delete) можно выполнять как легковесный `UPDATE` вместо мутации `ALTER UPDATE`. Работа легковесного `DELETE` управляется настройкой [`lightweight_delete_mode`](/ru/operations/settings/settings#lightweight_delete_mode).

<div id="performance-considerations">
  ## Особенности производительности
</div>

**Преимущества легковесных обновлений:**

* Задержка обновления сопоставима с задержкой запроса `INSERT ... SELECT ...`
* Записываются только обновлённые столбцы и значения, а не столбцы целиком в частях данных
* Не нужно ждать завершения выполняющихся в данный момент слияний/мутаций, поэтому задержка обновления предсказуема
* Возможно параллельное выполнение легковесных обновлений

**Возможное влияние на производительность:**

* Добавляет накладные расходы для запросов `SELECT`, к которым нужно применять патчи
* [Индексы пропуска данных](/ru/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) не будут использоваться для столбцов в частях данных, к которым нужно применить патчи. [Проекции](/ru/engines/table-engines/mergetree-family/mergetree.md/#projections) не будут использоваться, если у таблицы есть патч-части, в том числе для частей данных, к которым не нужно применять патчи.
* Небольшие обновления, выполняемые слишком часто, могут привести к ошибке &quot;too many parts&quot;. Рекомендуется объединять несколько обновлений в один запрос, например указав идентификаторы обновляемых строк в одном выражении `IN` в предложении `WHERE`
* Легковесные обновления предназначены для обновления небольшого количества строк (примерно до 10% таблицы). Если вам нужно обновить больший объём, рекомендуется использовать мутацию [`ALTER TABLE ... UPDATE`](/ru/sql-reference/statements/alter/update)

<div id="concurrent-operations">
  ## Параллельные операции
</div>

Легковесные обновления, в отличие от тяжёлых мутаций, не дожидаются завершения текущих слияний и мутаций.
Согласованность параллельно выполняемых легковесных обновлений регулируется настройками [`update_sequential_consistency`](/ru/operations/settings/settings#update_sequential_consistency) и [`update_parallel_mode`](/ru/operations/settings/settings#update_parallel_mode).

<div id="update-permissions">
  ## Разрешения для UPDATE
</div>

Для `UPDATE` требуется привилегия `ALTER UPDATE`. Чтобы разрешить указанному пользователю выполнять операторы `UPDATE` для конкретной таблицы, выполните:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## Детали реализации
</div>

Патч-части устроены так же, как обычные части, но содержат только обновлённые столбцы и несколько системных столбцов:

* `_part` - имя исходной части
* `_part_offset` - номер строки в исходной части
* `_block_number` - номер блока строки в исходной части
* `_block_offset` - смещение строки в блоке исходной части
* `_data_version` - версия обновлённых данных (номер блока, выделенный для запроса `UPDATE`)

В среднем это даёт около 40 байт накладных расходов на каждую обновлённую строку в патч-частях (в несжатом виде).
Системные столбцы помогают находить строки в исходной части, которые нужно обновить.
Системные столбцы связаны с [виртуальными столбцами](/ru/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) в исходной части, которые добавляются при чтении, если нужно применить патч-части.
Патч-части сортируются по `_part` и `_part_offset`.

Патч-части относятся к другим партициям, чем исходная часть.
Идентификатор партиции патч-части имеет вид `patch-<hash of column names in patch part>-<original_partition_id>`.
Поэтому патч-части с разными столбцами хранятся в разных партициях.
Например, три обновления `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>` и `SET x = 1, y = 1 WHERE <cond>` создадут три патч-части в трёх разных партициях.

Патч-части могут сливаться между собой, чтобы уменьшить количество применяемых патчей в запросах `SELECT` и снизить накладные расходы. При слиянии патч-частей используется алгоритм слияния [ReplacingMergeTree](/ru/engines/table-engines/mergetree-family/replacingmergetree) с `_data_version` в качестве столбца версии.
Поэтому патч-части всегда хранят последнюю версию для каждой обновлённой строки в части.

Легковесные обновления не ждут завершения выполняющихся в данный момент слияний и мутаций и всегда используют текущий снимок частей данных, чтобы выполнить обновление и создать патч-часть.
Из-за этого возможны два случая применения патч-частей.

Например, если мы читаем часть `A`, нам нужно применить патч-часть `X`:

* если `X` содержит саму часть `A`. Это происходит, если `A` не участвовала в слиянии на момент выполнения `UPDATE`.
* если `X` содержит части `B` и `C`, которые покрывает часть `A`. Это происходит, если во время выполнения `UPDATE` шло слияние (`B`, `C`) -&gt; `A`.

Для этих двух случаев есть два соответствующих способа применения патч-частей:

* Использовать слияние по отсортированным столбцам `_part`, `_part_offset`.
* Использовать JOIN по столбцам `_block_number`, `_block_offset`.

Режим JOIN медленнее и требует больше памяти, чем режим слияния, но используется реже.

<div id="related-content">
  ## Связанные материалы
</div>

* [`ALTER UPDATE`](/ru/sql-reference/statements/alter/update) - Ресурсоёмкие операции `UPDATE`
* [Легковесный `DELETE`](/ru/sql-reference/statements/delete) - Операции легковесного `DELETE`
* [`APPLY PATCHES`](/ru/sql-reference/statements/alter/apply-patches) - Принудительная физическая материализация патчей в частях данных (операция мутации)