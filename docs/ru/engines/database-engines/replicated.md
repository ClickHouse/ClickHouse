
# [экспериментальный] Replicated {#replicated}

Поддерживает репликацию метаданных через журнал DDL, записываемый в ZooKeeper и выполняемый на всех репликах для данной базы данных.
На одном сервере Clickhouse может одновременно работать и обновляться несколько реплицированных баз данных. Но не может существовать нескольких реплик одной и той же реплицированной базы данных.

## Создание базы данных {#creating-a-database}
``` sql
    CREATE DATABASE testdb ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Параметры движка**

-   `zoo_path` — путь в ZooKeeper. Один и тот же путь ZooKeeper соответствует одной и той же базе данных.
-   `shard_name` — Имя шарда. Реплики базы данных группируются в шарды по имени.
-   `replica_name` — Имя реплики. Имена реплик должны быть разными для всех реплик одного и того же шарда.

Using this engine, creation of Replicated tables requires no ZooKeeper path and replica name parameters. Table's replica name is the same as database replica name. Table's ZooKeeper path is a concatenation of database's ZooKeeper path, `/tables/`, and `UUID` of the table.

## Specifics and recommendations {#specifics-and-recommendations}

Algorithms
Specifics of read and write processes
Examples of tasks
Recommendations for usage
Specifics of data storage

## Usage Example {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.

**See Also** 

-   [link](#)
