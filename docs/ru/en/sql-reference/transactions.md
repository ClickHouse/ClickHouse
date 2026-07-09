---
description: 'Страница с описанием поддержки транзакций (ACID) в ClickHouse'
slug: /guides/developer/transactional
title: 'Поддержка транзакций (ACID)'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="transactional-acid-support">
  # Поддержка транзакций (ACID)
</div>

<div id="case-1-insert-into-one-partition-of-one-table-of-the-mergetree-family">
  ## Случай 1: INSERT в одну партицию одной таблицы семейства MergeTree*
</div>

Это транзакционная операция (ACID), если вставляемые строки упакованы и вставляются одним блоком (см. примечания):

* Атомарность: INSERT либо выполняется целиком, либо целиком отклоняется: если клиенту отправлено подтверждение, значит вставлены все строки; если клиенту отправлена ошибка, значит не вставлена ни одна строка.
* Согласованность: если ограничения таблицы не нарушены, то вставляются все строки из INSERT и INSERT завершается успешно; если ограничения нарушены, то не вставляется ни одна строка.
* Изолированность: параллельные клиенты видят согласованный снимок таблицы — состояние таблицы либо до попытки INSERT, либо после успешного INSERT; частичное состояние не наблюдается. Клиенты внутри другой транзакции имеют [изоляцию снимков](https://en.wikipedia.org/wiki/Snapshot_isolation), тогда как клиенты вне транзакции имеют уровень изоляции [чтение незафиксированных данных](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_uncommitted).
* Долговечность: успешный INSERT записывается в файловую систему до отправки ответа клиенту, на одной реплике или нескольких репликах (это контролируется настройкой `insert_quorum`), и ClickHouse может запросить у ОС синхронизацию данных файловой системы с носителем хранилища (это контролируется настройкой `fsync_after_insert`).
* INSERT в несколько таблиц одним оператором возможен, если задействованы materialized view (INSERT от клиента выполняется в таблицу, с которой связаны materialized view).

<div id="case-2-insert-into-multiple-partitions-of-one-table-of-the-mergetree-family">
  ## Случай 2: INSERT в несколько партиций одной таблицы семейства MergeTree*
</div>

То же, что и в случае 1 выше, со следующей особенностью:

* Если у таблицы много партиций и INSERT охватывает несколько партиций, то вставка в каждую партицию выполняется как отдельная транзакция

<div id="case-3-insert-into-one-distributed-table-of-the-mergetree-family">
  ## Случай 3: INSERT в одну distributed таблицу семейства MergeTree*
</div>

То же, что и в случае 1 выше, но с таким уточнением:

* INSERT в Distributed-таблицу в целом не является транзакционным, тогда как вставка в каждый сегмент транзакционна

<div id="case-4-using-a-buffer-table">
  ## Сценарий 4: Использование таблицы Buffer
</div>

* операции вставки в таблицы Buffer не обладают ни атомарностью, ни изолированностью, ни согласованностью, ни долговечностью

<div id="case-5-using-async_insert">
  ## Случай 5: Использование async_insert
</div>

То же, что и в случае 1 выше, но с одной оговоркой:

* атомарность обеспечивается, даже если `async_insert` включен, а `wait_for_async_insert` имеет значение 1 (по умолчанию); однако если `wait_for_async_insert` имеет значение 0, атомарность не гарантируется.

<div id="notes">
  ## Примечания
</div>

* строки, вставляемые клиентом в одном из форматов данных, упаковываются в один блок, если:
  * формат вставки построчный (например, CSV, TSV, Values, JSONEachRow и т. д.), а данные содержат менее `max_insert_block_size` строк (~1 000 000 по умолчанию) или менее `min_chunk_bytes_for_parallel_parsing` байт (10 МБ по умолчанию), если используется параллельный разбор (включен по умолчанию)
  * формат вставки столбцовый (например, Native, Parquet, ORC и т. д.), а данные содержат только один блок данных
* размер вставленного блока в общем случае может зависеть от множества настроек (например: `max_block_size`, `max_insert_block_size`, `min_insert_block_size_rows`, `min_insert_block_size_bytes`, `preferred_block_size_bytes` и т. д.)
* если клиент не получил ответ от сервера, он не знает, была ли транзакция успешно завершена, и может повторить ее, используя свойства вставки exactly-once
* ClickHouse внутренне использует [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) с [изоляцией снимков](https://en.wikipedia.org/wiki/Snapshot_isolation) для параллельных транзакций
* все свойства ACID сохраняются даже в случае принудительного завершения работы/сбоя сервера
* для обеспечения надежных вставок в типичной конфигурации должен быть включен либо insert&#95;quorum для разных AZ, либо fsync
* «согласованность» в терминах ACID не охватывает семантику распределенных систем, см. https://jepsen.io/consistency; она регулируется другими настройками (select&#95;sequential&#95;consistency)
* это объяснение не охватывает новую возможность транзакций, которая позволяет выполнять полнофункциональные транзакции для нескольких таблиц, materialized view, нескольких SELECT и т. д. (см. следующий раздел о Transactions, Commit, and Rollback)

<div id="transactions-commit-and-rollback">
  ## Транзакции, коммит и откат
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Помимо функциональности, описанной в начале этого документа, ClickHouse также предоставляет экспериментальную поддержку транзакций, коммита и отката.

<div id="requirements">
  ### Требования
</div>

* Разверните ClickHouse Keeper или ZooKeeper для отслеживания транзакций
* Только БД Atomic (по умолчанию)
* Только движок таблицы MergeTree без репликации
* Включите экспериментальную поддержку транзакций, добавив следующий параметр в `config.d/transactions.xml`:
  ```xml
  <clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
  </clickhouse>
  ```

<div id="notes-1">
  ### Примечания
</div>

* Это экспериментальная возможность, и в дальнейшем возможны изменения.
* Если во время транзакции возникает исключение, выполнить коммит транзакции нельзя.  Это относится ко всем исключениям, включая `UNKNOWN_FUNCTION`, вызванные опечатками.
* Вложенные транзакции не поддерживаются; завершите текущую транзакцию и вместо неё начните новую

<div id="configuration">
  ### Конфигурация
</div>

В этих примерах используется одиночный сервер ClickHouse с включенным ClickHouse Keeper.

<div id="enable-experimental-transaction-support">
  #### Включение экспериментальной поддержки транзакций
</div>

```xml title=/etc/clickhouse-server/config.d/transactions.xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

<div id="basic-configuration-for-a-single-clickhouse-server-node-with-clickhouse-keeper-enabled">
  #### Базовая конфигурация для одного узла сервера ClickHouse с включенным ClickHouse Keeper
</div>

:::note
Подробные сведения о развертывании сервера ClickHouse и необходимом кворуме узлов ClickHouse Keeper см. в документации по [развертыванию](/ru/deployment-guides/terminology.md). Приведенная здесь конфигурация предназначена для экспериментальных целей.
:::

```xml title=/etc/clickhouse-server/config.d/config.xml
<clickhouse replace="true">
    <logger>
        <level>debug</level>
        <log>/var/log/clickhouse-server/clickhouse-server.log</log>
        <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <display_name>node 1</display_name>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <zookeeper>
        <node>
            <host>clickhouse-01</host>
            <port>9181</port>
        </node>
    </zookeeper>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse-keeper-01</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
```

<div id="example">
  ### Пример
</div>

<div id="verify-that-experimental-transactions-are-enabled">
  #### Убедитесь, что экспериментальные транзакции включены
</div>

Выполните `BEGIN TRANSACTION` или `START TRANSACTION`, а затем `ROLLBACK`, чтобы убедиться, что экспериментальные транзакции включены и ClickHouse Keeper тоже включен, поскольку он используется для отслеживания транзакций.

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

:::tip
Если вы видите следующую ошибку, проверьте файл конфигурации и убедитесь, что параметр `allow_experimental_transactions` установлен в значение `1` (или любое значение, отличное от `0` или `false`).

```response
Code: 48. DB::Exception: Received from localhost:9000.
DB::Exception: Transactions are not supported.
(NOT_IMPLEMENTED)
```

Вы также можете проверить ClickHouse Keeper, выполнив

```bash
echo ruok | nc localhost 9181
```

ClickHouse Keeper должен вернуть `imok`.
:::

```sql
ROLLBACK
```

```response
Ok.
```

<div id="create-a-table-for-testing">
  #### Создайте таблицу для тестов
</div>

:::tip
Создание таблиц не поддерживает транзакции. Выполните этот DDL-запрос вне транзакции.
:::

```sql
CREATE TABLE mergetree_table
(
    `n` Int64
)
ENGINE = MergeTree
ORDER BY n
```

```response
Ok.
```

<div id="begin-a-transaction-and-insert-a-row">
  #### Начните транзакцию и вставьте строку
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (10)
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 10 │
└────┘
```

:::note
Вы можете выполнить запрос к таблице в рамках транзакции и увидеть, что строка была вставлена, хотя коммит ещё не выполнен.
:::

<div id="rollback-the-transaction-and-query-the-table-again">
  #### Откатите транзакцию и снова выполните запрос к таблице
</div>

Убедитесь, что транзакция была откачена:

```sql
ROLLBACK
```

```response
Ok.
```

```sql
SELECT *
FROM mergetree_table
```

```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

<div id="complete-a-transaction-and-query-the-table-again">
  #### Завершите транзакцию и снова выполните запрос к таблице
</div>

```sql
BEGIN TRANSACTION
```

```response
Ok.
```

```sql
INSERT INTO mergetree_table FORMAT Values (42)
```

```response
Ok.
```

```sql
COMMIT
```

```response
Ok. Elapsed: 0.002 sec.
```

```sql
SELECT *
FROM mergetree_table
```

```response
┌──n─┐
│ 42 │
└────┘
```

<div id="transactions-introspection">
  ### Просмотр сведений о транзакциях
</div>

Вы можете просматривать транзакции, выполняя запрос к таблице `system.transactions`, но учтите, что к этой
таблице нельзя обращаться из сеанса, в котором открыта транзакция. Чтобы выполнить запрос к этой таблице, откройте второй сеанс `клиент ClickHouse`.

```sql
SELECT *
FROM system.transactions
FORMAT Vertical
```

```response
Row 1:
──────
tid:         (33,61,'51e60bce-6b82-4732-9e1d-b40705ae9ab8')
tid_hash:    11240433987908122467
elapsed:     210.017820947
is_readonly: 1
state:       RUNNING
```

<div id="more-details">
  ## Подробнее
</div>

См. эту [мета-задачу](https://github.com/ClickHouse/ClickHouse/issues/48794), где собраны гораздо более подробные тесты и публикуются обновления о ходе работ.