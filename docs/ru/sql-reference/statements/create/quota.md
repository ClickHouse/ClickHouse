---
toc_priority: 42
toc_title: "\u041a\u0432\u043e\u0442\u0430"
---

# CREATE QUOTA {#create-quota-statement}

Создает [квоту](../../../operations/access-rights.md#quotas-management), которая может быть присвоена пользователю или роли.

### Синтаксис {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Пример {#create-quota-example}

Ограничить максимальное количество запросов для текущего пользователя до 123 запросов каждые 15 месяцев:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/quota) 
<!--hide-->