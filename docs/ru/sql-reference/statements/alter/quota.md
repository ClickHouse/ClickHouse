---
toc_priority: 46
toc_title: QUOTA
---

# ALTER QUOTA {#alter-quota-statement}

Изменяет [квоту](../../../operations/access-rights.md#quotas-management).

Синтаксис:

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {USER_NAME | IP_ADDRESS | CLIENT_KEY | CLIENT_KEY, USER_NAME | CLIENT_KEY, IP_ADDRESS} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT_ROWS | RESULT_BYTES | READ_ROWS | READ_BYTES | EXECUTION_TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

**Примеры**

Ограничить для текущего пользователя максимальное число запросов — не более 123 запросов за каждые 15 месяцев:

``` sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER;
```

Ограничить по умолчанию максимальное время выполнения запроса — не более полсекунды за каждые 30 минут, а также максимальное число запросов — не более 321 и максимальное число ошибок — не более 10 за каждые 5 кварталов:

``` sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 MINUTE MAX EXECUTION_TIME = 0.5, FOR INTERVAL 5 QUATER MAX QUERIES = 321, ERRORS = 10 TO default;
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/alter/quota/) <!--hide-->
