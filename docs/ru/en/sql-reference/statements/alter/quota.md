---
description: 'Документация по QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

Изменяет квоты.

Синтаксис:

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Ключи `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address` и `normalized_query_hash` соответствуют полям таблицы [system.quotas](../../../operations/system-tables/quotas.md).

Параметры `IPV4_PREFIX_BITS` и `IPV6_PREFIX_BITS` можно использовать только когда `KEYED BY` имеет значение `ip_address` или `forwarded_ip_address`. Они соответствуют полю таблицы [system.quotas](../../../operations/system-tables/quotas.md).

Параметры `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `queries_per_normalized_hash` соответствуют полям таблицы [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

Предложение `ON CLUSTER` позволяет создавать квоты в кластере, см. [распределённый DDL](../../../sql-reference/distributed-ddl.md).

**Примеры**

Ограничьте максимальное число запросов для текущего пользователя до 123 за 15 месяцев:

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Для пользователя default ограничьте максимальное время выполнения половиной секунды в течение 30 минут, а также максимальное количество запросов — до 321, а максимальное количество ошибок — до 10 в течение 5 кварталов:

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```