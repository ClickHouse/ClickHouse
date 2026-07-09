---
description: 'Documentación sobre QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

Crea una [cuota](../../../guides/sre/user-management/index.md#quotas-management) que se puede asignar a un usuario o a un rol.

Sintaxis:

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

Las claves `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address` y `normalized_query_hash` se corresponden con los campos de la tabla [system.quotas](../../../operations/system-tables/quotas.md).

Las opciones `IPV4_PREFIX_BITS` y `IPV6_PREFIX_BITS` solo se pueden usar cuando `KEYED BY` es `ip_address` o `forwarded_ip_address`. Se corresponden con el campo de la tabla [system.quotas](../../../operations/system-tables/quotas.md).

Los parámetros `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `written_bytes`, `execution_time`, `failed_sequential_authentications`, `queries_per_normalized_hash` se corresponden con los campos de la tabla [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

La cláusula `ON CLUSTER` permite crear cuotas en un clúster; consulte [DDL distribuido](../../../sql-reference/distributed-ddl.md).

**Ejemplos**

Limite el número máximo de consultas para el usuario actual mediante una restricción de 123 consultas en 15 meses:

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Para el usuario `default`, limite el tiempo de ejecución máximo a medio segundo cada 30 minutos, y limite el número máximo de consultas a 321 y el número máximo de errores a 10 cada 5 cuartos de hora:

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

Cree una cuota en la que cada patrón de consulta normalizado distinto tenga su propio bucket, con un límite de 100 ejecuciones por hora:

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

Limite cada patrón de consulta normalizado a un máximo de 50 ejecuciones por hora (independientemente del tipo de quota key):

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

Se pueden encontrar más ejemplos con la configuración XML (no compatible con ClickHouse Cloud) en la [guía de cuotas](/es/operations/quotas).

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo crear aplicaciones de una sola página con ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)