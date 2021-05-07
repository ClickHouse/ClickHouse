---
toc_priority: 5
toc_title: MongoDB
---

# MongoDB {#mongodb}

Движок таблиц MongoDB позволяет читать данные из коллекций СУБД MongoDB. В таблицах допустимы только плоские (не вложенные) типы данных. Запись (`INSERT`-запросы) не поддерживается.

## Создание таблицы {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password);
```

**Параметры движка**

-   `host:port` — адрес сервера MongoDB.

-   `database` — имя базы данных на удалённом сервере.

-   `collection` — имя коллекции на удалённом сервере.

-   `user` — пользователь MongoDB.

-   `password` — пароль пользователя.

## Примеры использования {#usage-example}

Таблица в ClickHouse для чтения данных из колекции MongoDB:

``` text
CREATE TABLE mongo_table
(
    key UInt64, 
    data String
) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'testuser', 'clickhouse');
```

Запрос к таблице:

``` sql
SELECT COUNT() FROM mongo_table;
```

``` text
┌─count()─┐
│       4 │
└─────────┘
```

[Original article](https://clickhouse.tech/docs/ru/engines/table-engines/integrations/mongodb/) <!--hide-->
