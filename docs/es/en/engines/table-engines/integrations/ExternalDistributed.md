---
description: 'El motor `ExternalDistributed` permite realizar consultas `SELECT`
  sobre datos almacenados en servidores MySQL o PostgreSQL remotos. Acepta motores MySQL o
  PostgreSQL como argumento, por lo que permite el sharding.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'Motor de tabla ExternalDistributed'
doc_type: 'reference'
---

El motor `ExternalDistributed` permite realizar consultas `SELECT` sobre datos almacenados en servidores MySQL o PostgreSQL remotos. Acepta motores [MySQL](../../../engines/table-engines/integrations/mysql.md) o [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) como argumento, por lo que permite el sharding.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

Consulta una descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

La estructura de la tabla puede diferir de la estructura de la tabla original:

* Los nombres de las columnas deben ser los mismos que en la tabla original, pero puede usar solo algunas de ellas y en cualquier orden.
* Los tipos de columna pueden diferir de los de la tabla original. ClickHouse intenta [convertir](/es/sql-reference/functions/type-conversion-functions#CAST) los valores a los tipos de datos de ClickHouse.

**Parámetros del motor**

* `engine` — El motor de tabla: `MySQL` o `PostgreSQL`.
* `host:port` — Dirección del servidor MySQL o PostgreSQL.
* `database` — Nombre de la base de datos remota.
* `table` — Nombre de la tabla remota.
* `user` — Nombre de usuario.
* `password` — Contraseña del usuario.

<div id="implementation-details">
  ## Detalles de implementación
</div>

Admite varias réplicas, que deben enumerarse con `|`, y los segmentos deben enumerarse con `,`. Por ejemplo:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

Al especificar réplicas, se selecciona una de las réplicas disponibles para cada uno de los segmentos durante la lectura. Si la conexión falla, se selecciona la siguiente réplica, y así sucesivamente hasta recorrer todas las réplicas. Si el intento de conexión falla para todas las réplicas, se repite varias veces de la misma manera.

Puede especificar cualquier número de segmentos y cualquier número de réplicas para cada segmento.

**Véase también**

* [motor de tabla MySQL](../../../engines/table-engines/integrations/mysql.md)
* [motor de tabla PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
* [motor de tabla Distributed](../../../engines/table-engines/special/distributed.md)