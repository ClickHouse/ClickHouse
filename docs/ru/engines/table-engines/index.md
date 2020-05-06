# Движки таблиц {#table_engines}

Движок таблицы (тип таблицы) определяет:

-   Как и где хранятся данные, куда их писать и откуда читать.
-   Какие запросы поддерживаются и каким образом.
-   Конкурентный доступ к данным.
-   Использование индексов, если есть.
-   Возможно ли многопоточное выполнение запроса.
-   Параметры репликации данных.

## Семейства движков {#semeistva-dvizhkov}

### MergeTree {#mergetree}

Наиболее универсальные и функциональные движки таблиц для задач с высокой загрузкой. Общим свойством этих движков является быстрая вставка данных с последующей фоновой обработкой данных. Движки `*MergeTree` поддерживают репликацию данных (в [Replicated\*](mergetree-family/replication.md) версиях движков), партиционирование, и другие возможности не поддержанные для других движков.

Движки семейства:

-   [MergeTree](mergetree-family/mergetree.md)
-   [ReplacingMergeTree](mergetree-family/replacingmergetree.md)
-   [SummingMergeTree](mergetree-family/summingmergetree.md)
-   [AggregatingMergeTree](mergetree-family/aggregatingmergetree.md)
-   [CollapsingMergeTree](mergetree-family/collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](mergetree-family/versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](mergetree-family/graphitemergetree.md)

### Log {#log}

Простые [движки](log-family/index.md) с минимальной функциональностью. Они наиболее эффективны, когда вам нужно быстро записать много небольших таблиц (до примерно 1 миллиона строк) и прочитать их позже целиком.

Движки семейства:

-   [TinyLog](log-family/tinylog.md)
-   [StripeLog](log-family/stripelog.md)
-   [Log](log-family/log.md)

### Движки для интеграции {#dvizhki-dlia-integratsii}

Движки для связи с другими системами хранения и обработки данных.

Движки семейства:

-   [Kafka](integrations/kafka.md)
-   [MySQL](integrations/mysql.md)
-   [ODBC](integrations/odbc.md)
-   [JDBC](integrations/jdbc.md)

### Специальные движки {#spetsialnye-dvizhki}

Движки семейства:

-   [Distributed](special/distributed.md)
-   [MaterializedView](special/materializedview.md)
-   [Dictionary](special/dictionary.md)
-   [Merge](special/merge.md)
-   [File](special/file.md)
-   [Null](special/null.md)
-   [Set](special/set.md)
-   [Join](special/join.md)
-   [URL](special/url.md)
-   [View](special/view.md)
-   [Memory](special/memory.md)
-   [Buffer](special/buffer.md)

## Виртуальные столбцы {#table_engines-virtual-columns}

Виртуальный столбец — это неотъемлемый атрибут движка таблиц, определенный в исходном коде движка.

Виртуальные столбцы не надо указывать в запросе `CREATE TABLE` и их не отображаются в результатах запросов `SHOW CREATE TABLE` и `DESCRIBE TABLE`. Также виртуальные столбцы доступны только для чтения, поэтому вы не можете вставлять в них данные.

Чтобы получить данные из виртуального столбца, необходимо указать его название в запросе `SELECT`. `SELECT *` не отображает данные из виртуальных столбцов.

При создании таблицы со столбцом, имя которого совпадает с именем одного из виртуальных столбцов таблицы, виртуальный столбец становится недоступным. Не делайте так. Чтобы помочь избежать конфликтов, имена виртуальных столбцов обычно предваряются подчеркиванием.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/) <!--hide-->
