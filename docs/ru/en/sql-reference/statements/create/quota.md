---
description: 'Документация по QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

Создаёт [квоту](../../../guides/sre/user-management/index.md#quotas-management), которую можно назначить пользователю или роли.

Синтаксис:

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | written_bytes | execution_time | failed_sequential_authentications | queries_per_normalized_hash} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Ключи `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address` и `normalized_query_hash` соответствуют полям таблицы [system.quotas](../../../operations/system-tables/quotas.md).

Параметры `IPV4_PREFIX_BITS` и `IPV6_PREFIX_BITS` можно использовать только тогда, когда `KEYED BY` имеет значение `ip_address` или `forwarded_ip_address`. Они соответствуют полю таблицы [system.quotas](../../../operations/system-tables/quotas.md).

Параметры `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `written_bytes`, `execution_time`, `failed_sequential_authentications`, `queries_per_normalized_hash` соответствуют полям таблицы [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

Предложение `ON CLUSTER` позволяет создавать квоты в кластере, см. [Distributed DDL](../../../sql-reference/distributed-ddl.md).

**Примеры**

Ограничьте максимальное число запросов для текущего пользователя до 123 за 15 месяцев:

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Для пользователя `default` задайте ограничение: максимальное время выполнения — полсекунды за 30 минут, максимальное количество запросов — 321, а максимальное количество ошибок — 10 за 5 кварталов:

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

Создайте QUOTA, в которой для каждого отдельного нормализованного шаблона запроса используется собственный бакет с ограничением 100 выполнений в час:

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

Ограничьте каждый отдельный нормализованный шаблон запроса максимум 50 выполнениями в час (независимо от типа ключа квоты):

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

Дополнительные примеры с использованием XML-конфигурации (не поддерживается в ClickHouse Cloud) см. в [руководстве по квотам](/ru/operations/quotas).

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Создание одностраничных приложений с ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)