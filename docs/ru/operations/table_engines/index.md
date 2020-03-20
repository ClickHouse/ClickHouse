# Движки таблиц {#table-engines}

Движок таблицы (тип таблицы) определяет:

-   Как и где хранятся данные, куда их писать и откуда читать.
-   Какие запросы поддерживаются и каким образом.
-   Конкурентный доступ к данным.
-   Использование индексов, если есть.
-   Возможно ли многопоточное выполнение запроса.
-   Параметры репликации данных.

## Семейства движков {#semeistva-dvizhkov}

### MergeTree {#mergetree}

Наиболее универсальные и функциональные движки таблиц для задач с высокой загрузкой. Общим свойством этих движков является быстрая вставка данных с последующей фоновой обработкой данных. Движки `*MergeTree` поддерживают репликацию данных (в [Replicated\*](replication.md) версиях движков), партиционирование, и другие возможности не поддержанные для других движков.

Движки семейства:

-   [MergeTree](mergetree.md)
-   [ReplacingMergeTree](replacingmergetree.md)
-   [SummingMergeTree](summingmergetree.md)
-   [AggregatingMergeTree](aggregatingmergetree.md)
-   [CollapsingMergeTree](collapsingmergetree.md)
-   [VersionedCollapsingMergeTree](versionedcollapsingmergetree.md)
-   [GraphiteMergeTree](graphitemergetree.md)

### Log {#log}

Простые [движки](log_family.md) с минимальной функциональностью. Они наиболее эффективны, когда вам нужно быстро записать много небольших таблиц (до примерно 1 миллиона строк) и прочитать их позже целиком.

Движки семейства:

-   [TinyLog](tinylog.md)
-   [StripeLog](stripelog.md)
-   [Log](log.md)

### Движки для интергации {#dvizhki-dlia-intergatsii}

Движки для связи с другими системами хранения и обработки данных.

Движки семейства:

-   [Kafka](kafka.md)
-   [MySQL](mysql.md)
-   [ODBC](odbc.md)
-   [JDBC](jdbc.md)

### Специальные движки {#spetsialnye-dvizhki}

Движки семейства:

-   [Distributed](distributed.md)
-   [MaterializedView](materializedview.md)
-   [Dictionary](dictionary.md)
-   [Merge](merge.md)
-   [File](file.md)
-   [Null](null.md)
-   [Set](set.md)
-   [Join](join.md)
-   [URL](url.md)
-   [View](view.md)
-   [Memory](memory.md)
-   [Buffer](buffer.md)

## Виртуальные столбцы {#table-engines-virtual-columns}

Виртуальный столбец — это неотъемлемый атрибут движка таблиц, определенный в исходном коде движка.

Виртуальные столбцы не надо указывать в запросе `CREATE TABLE` и их не отображаются в результатах запросов `SHOW CREATE TABLE` и `DESCRIBE TABLE`. Также виртуальные столбцы доступны только для чтения, поэтому вы не можете вставлять в них данные.

Чтобы получить данные из виртуального столбца, необходимо указать его название в запросе `SELECT`. `SELECT *` не отображает данные из виртуальных столбцов.

При создании таблицы со столбцом, имя которого совпадает с именем одного из виртуальных столбцов таблицы, виртуальный столбец становится недоступным. Не делайте так. Чтобы помочь избежать конфликтов, имена виртуальных столбцов обычно предваряются подчеркиванием.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/) <!--hide-->
