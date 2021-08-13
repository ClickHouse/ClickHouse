---
toc_folder_title: "Движки таблиц"
toc_priority: 26
toc_title: "Введение"
---


# Движки таблиц {#table_engines}

Движок таблицы (тип таблицы) определяет:

-   Как и где хранятся данные, куда их писать и откуда читать.
-   Какие запросы поддерживаются и каким образом.
-   Конкурентный доступ к данным.
-   Использование индексов, если есть.
-   Возможно ли многопоточное выполнение запроса.
-   Параметры репликации данных.

## Семейства движков {#engine-families}

### MergeTree {#mergetree}

Наиболее универсальные и функциональные движки таблиц для задач с высокой загрузкой. Общим свойством этих движков является быстрая вставка данных с последующей фоновой обработкой данных. Движки `*MergeTree` поддерживают репликацию данных (в [Replicated\*](mergetree-family/replication.md#replication) версиях движков), партиционирование, и другие возможности не поддержанные для других движков.

Движки семейства:

-   [MergeTree](mergetree-family/mergetree.md#mergetree)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md#replacingmergetree)
-   [SummingMergeTree](mergetree-family/summingmergetree.md#summingmergetree)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md#aggregatingmergetree)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md#versionedcollapsingmergetree)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md#graphitemergetree)

### Log {#log}

Простые [движки](log-family/index.md) с минимальной функциональностью. Они наиболее эффективны, когда вам нужно быстро записать много небольших таблиц (до примерно 1 миллиона строк) и прочитать их позже целиком.

Движки семейства:

-   [TinyLog](log-family/tinylog.md#tinylog)
-   [StripeLog](log-family/stripelog.md#stripelog)
-   [Log](log-family/log.md#log)

### Движки для интеграции {#integration-engines}

Движки для связи с другими системами хранения и обработки данных.

Движки семейства:

-   [Kafka](integrations/kafka.md#kafka)
-   [MySQL](integrations/mysql.md#mysql)
-   [ODBC](integrations/odbc.md#table-engine-odbc)
-   [JDBC](integrations/jdbc.md#table-engine-jdbc)
-   [S3](integrations/s3.md#table-engine-s3)

### Специальные движки {#spetsialnye-dvizhki}

-   [ODBC](../../engines/table-engines/integrations/odbc.md)
-   [JDBC](../../engines/table-engines/integrations/jdbc.md)
-   [MySQL](../../engines/table-engines/integrations/mysql.md)
-   [MongoDB](../../engines/table-engines/integrations/mongodb.md)
-   [HDFS](../../engines/table-engines/integrations/hdfs.md)
-   [Kafka](../../engines/table-engines/integrations/kafka.md)
-   [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md)
-   [RabbitMQ](../../engines/table-engines/integrations/rabbitmq.md)
-   [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)

### Специальные движки {#special-engines}

Движки семейства:

-   [Distributed](special/distributed.md#distributed)
-   [MaterializedView](special/materializedview.md#materializedview)
-   [Dictionary](special/dictionary.md#dictionary)
-   [Merge](special/merge.md#merge)
-   [File](special/file.md#file)
-   [Null](special/null.md#null)
-   [Set](special/set.md#set)
-   [Join](special/join.md#join)
-   [URL](special/url.md#table_engines-url)
-   [View](special/view.md#table_engines-view)
-   [Memory](special/memory.md#memory)
-   [Buffer](special/buffer.md#buffer)

## Виртуальные столбцы {#table_engines-virtual_columns}

Виртуальный столбец — это неотъемлемый атрибут движка таблиц, определенный в исходном коде движка.

Виртуальные столбцы не надо указывать в запросе `CREATE TABLE` и их не отображаются в результатах запросов `SHOW CREATE TABLE` и `DESCRIBE TABLE`. Также виртуальные столбцы доступны только для чтения, поэтому вы не можете вставлять в них данные.

Чтобы получить данные из виртуального столбца, необходимо указать его название в запросе `SELECT`. `SELECT *` не отображает данные из виртуальных столбцов.

При создании таблицы со столбцом, имя которого совпадает с именем одного из виртуальных столбцов таблицы, виртуальный столбец становится недоступным. Не делайте так. Чтобы помочь избежать конфликтов, имена виртуальных столбцов обычно предваряются подчеркиванием.
