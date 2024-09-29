---
slug: /ru/sql-reference/statements/create/quota
sidebar_position: 42
sidebar_label: "Квота"
---

# CREATE QUOTA {#create-quota-statement}

Создает [квоту](../../../operations/access-rights.md#quotas-management), которая может быть присвоена пользователю или роли.

Синтаксис:

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | client_key | client_key,user_name | client_key,ip_address} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```
Ключи `user_name`, `ip_address`, `client_key`, `client_key, user_name` и `client_key, ip_address` соответствуют полям таблицы [system.quotas](../../../operations/system-tables/quotas.md).

Параметры `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `failed_sequential_authentications` соответствуют полям таблицы [system.quotas_usage](../../../operations/system-tables/quotas_usage.md).

В секции `ON CLUSTER` можно указать кластеры, на которых создается квота, см. [Распределенные DDL запросы](../../../sql-reference/distributed-ddl.md).

**Примеры**

Ограничить максимальное количество запросов для текущего пользователя — не более 123 запросов за каждые 15 месяцев:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Ограничить по умолчанию максимальное время выполнения запроса — не более полсекунды за каждые 30 минут, а также максимальное число запросов — не более 321 и максимальное число ошибок — не более 10 за каждые 5 кварталов:

``` sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```