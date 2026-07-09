---
description: 'Документация по движкам таблиц'
slug: /engines/table-engines/
toc_folder_title: 'Движки таблиц'
toc_priority: 26
toc_title: 'Введение'
title: 'Движки таблиц'
doc_type: 'reference'
---

Движок таблицы (тип таблицы) определяет:

* Как и где хранятся данные, куда они записываются и откуда читаются.
* Какие запросы поддерживаются и каким образом.
* Одновременный доступ к данным.
* Использование индексов, если они предусмотрены.
* Возможно ли многопоточное выполнение запросов.
* Параметры репликации данных.

<div id="engine-families">
  ## Семейства движков
</div>

<div id="mergetree">
  ### MergeTree
</div>

Наиболее универсальные и функциональные движки таблиц для задач с высокой нагрузкой. Общая особенность этих движков — быстрая вставка данных с их последующей фоновой обработкой. Движки семейства `MergeTree` поддерживают репликацию данных (в версиях движков [Replicated*](/ru/engines/table-engines/mergetree-family/replication)), партиционирование, вторичные индексы пропуска данных и другие возможности, недоступные в других движках.

Движки семейства:

| Движки семейства MergeTree                                                                           |
| ---------------------------------------------------------------------------------------------------- |
| [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree)                                       |
| [ReplacingMergeTree](/ru/engines/table-engines/mergetree-family/replacingmergetree)                     |
| [SummingMergeTree](/ru/engines/table-engines/mergetree-family/summingmergetree)                         |
| [AggregatingMergeTree](/ru/engines/table-engines/mergetree-family/aggregatingmergetree)                 |
| [CollapsingMergeTree](/ru/engines/table-engines/mergetree-family/collapsingmergetree)                   |
| [VersionedCollapsingMergeTree](/ru/engines/table-engines/mergetree-family/versionedcollapsingmergetree) |
| [GraphiteMergeTree](/ru/engines/table-engines/mergetree-family/graphitemergetree)                       |
| [CoalescingMergeTree](/ru/engines/table-engines/mergetree-family/coalescingmergetree)                   |

<div id="log">
  ### Log
</div>

Легковесные [движки](../../engines/table-engines/log-family/index.md) с минимальным набором функций. Они наиболее эффективны, когда нужно быстро записывать множество небольших таблиц (примерно до 1 миллиона строк), а затем читать их целиком.

Движки семейства:

| Движки семейства Log                                     |
| -------------------------------------------------------- |
| [TinyLog](/ru/engines/table-engines/log-family/tinylog)     |
| [StripeLog](/ru/engines/table-engines/log-family/stripelog) |
| [Log](/ru/engines/table-engines/log-family/log)             |

<div id="integration-engines">
  ### Интеграционные движки
</div>

Движки для взаимодействия с другими системами хранения и обработки данных.

Движки семейства:

| Интеграционные движки                                                           |
| ------------------------------------------------------------------------------- |
| [ODBC](../../engines/table-engines/integrations/odbc.md)                        |
| [JDBC](../../engines/table-engines/integrations/jdbc.md)                        |
| [MySQL](../../engines/table-engines/integrations/mysql.md)                      |
| [MongoDB](../../engines/table-engines/integrations/mongodb.md)                  |
| [Redis](../../engines/table-engines/integrations/redis.md)                      |
| [HDFS](../../engines/table-engines/integrations/hdfs.md)                        |
| [S3](../../engines/table-engines/integrations/s3.md)                            |
| [Kafka](../../engines/table-engines/integrations/kafka.md)                      |
| [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) |
| [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)                |
| [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)            |
| [S3Queue](../../engines/table-engines/integrations/s3queue.md)                  |
| [TimeSeries](../../engines/table-engines/integrations/time-series.md)           |

<div id="special-engines">
  ### Специальные движки
</div>

Движки семейства:

| Специальные движки                                            |
| ------------------------------------------------------------- |
| [Distributed](/ru/engines/table-engines/special/distributed)     |
| [Dictionary](/ru/engines/table-engines/special/dictionary)       |
| [Merge](/ru/engines/table-engines/special/merge)                 |
| [Executable](/ru/engines/table-engines/special/executable)       |
| [File](/ru/engines/table-engines/special/file)                   |
| [Null](/ru/engines/table-engines/special/null)                   |
| [Set](/ru/engines/table-engines/special/set)                     |
| [Join](/ru/engines/table-engines/special/join)                   |
| [URL](/ru/engines/table-engines/special/url)                     |
| [View](/ru/engines/table-engines/special/view)                   |
| [Memory](/ru/engines/table-engines/special/memory)               |
| [Buffer](/ru/engines/table-engines/special/buffer)               |
| [External Data](/ru/engines/table-engines/special/external-data) |
| [GenerateRandom](/ru/engines/table-engines/special/generate)     |
| [KeeperMap](/ru/engines/table-engines/special/keeper-map)        |
| [FileLog](/ru/engines/table-engines/special/filelog)             |

<div id="table_engines-virtual_columns">
  ## Виртуальные столбцы
</div>

Виртуальный столбец — это неотъемлемый атрибут движка таблицы, определённый в исходном коде движка.

Виртуальные столбцы не следует указывать в запросе `CREATE TABLE`; они также не отображаются в результатах запросов `SHOW CREATE TABLE` и `DESCRIBE TABLE`. Кроме того, виртуальные столбцы доступны только для чтения, поэтому вставлять в них данные нельзя.

Чтобы выбрать данные из виртуального столбца, необходимо указать его имя в запросе `SELECT`. `SELECT *` не возвращает значения из виртуальных столбцов.

Если создать таблицу со столбцом, имя которого совпадает с именем одного из виртуальных столбцов таблицы, виртуальный столбец станет недоступен. Мы не рекомендуем так делать. Во избежание конфликтов имена виртуальных столбцов обычно начинаются с символа подчёркивания.

* `_table` — содержит имя таблицы, из которой были прочитаны данные. Тип: [String](../../sql-reference/data-types/string.md).

  Независимо от используемого движка таблицы, каждая таблица включает универсальный виртуальный столбец `_table`.

  При выполнении запроса к таблице с движком таблицы Merge можно задать константные условия для `_table` в предложении `WHERE/PREWHERE` (например, `WHERE _table='xyz'`). В этом случае чтение выполняется только для тех таблиц, для которых выполняется условие по `_table`, поэтому столбец `_table` действует как индекс.

  При использовании запросов вида `SELECT ... FROM (... UNION ALL ...)` можно определить, из какой именно таблицы происходят возвращаемые строки, указав столбец `_table`.