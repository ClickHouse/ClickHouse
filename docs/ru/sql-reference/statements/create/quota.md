---
toc_priority: 42
toc_title: "\u041a\u0432\u043e\u0442\u0430"
---

# CREATE QUOTA {#create-quota-statement}

Создает [квоту](../../../operations/access-rights.md#quotas-management), которая может быть присвоена пользователю или роли.

Синтаксис:

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {NONE | USER_NAME | IP_ADDRESS | CLIENT_KEY | CLIENT_KEY, USER_NAME | CLIENT_KEY, IP_ADDRESS} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT_ROWS | RESULT_BYTES | READ_ROWS | READ_BYTES | EXECUTION_TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

В секции `ON CLUSTER` можно указать кластеры, на которых создается квота, см. [Распределенные DDL запросы](../../../sql-reference/distributed-ddl.md).

Поддерживаются два варианта написания составных типов ключей: с подчеркиванием (`CLIENT_KEY`) или через пробел и в одинарных кавычках (`'client key'`). Также можно использовать ключ `'client key or user name'` вместо `CLIENT_KEY, USER_NAME`, и ключ `'client key or ip address'` вместо `CLIENT_KEY, IP_ADDRESS`.

Поддерживаются также два варианта написания составных типов ресурсов: с подчеркиванием (`RESULT_ROWS`) или без подчеркивания, через пробел (`RESULT ROWS`). 

**Примеры**

Ограничить максимальное количество запросов для текущего пользователя — не более 123 запросов за каждые 15 месяцев:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER;
```

Ограничить по умолчанию максимальное время выполнения запроса — не более полсекунды за каждые 30 минут, а также максимальное число запросов — не более 321 и максимальное число ошибок — не более 10 за каждые 5 кварталов:

``` sql
CREATE QUOTA qB FOR INTERVAL 30 MINUTE MAX EXECUTION_TIME = 0.5, FOR INTERVAL 5 QUATER MAX QUERIES = 321, ERRORS = 10 TO default;
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/quota) 
<!--hide-->