---
description: 'La función de tabla `remote` permite acceder a servidores remotos sobre la marcha,
  es decir, sin crear una tabla [Distributed](../../engines/table-engines/special/distributed.md). La función de tabla `remoteSecure` es igual
  que `remote`, pero mediante una conexión segura.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

La función de tabla `remote` permite acceder a servidores remotos sobre la marcha, es decir, sin crear una tabla [Distributed](../../engines/table-engines/special/distributed.md). La función de tabla `remoteSecure` es igual que `remote`, pero mediante una conexión segura.

Ambas funciones pueden utilizarse en consultas `SELECT` e `INSERT`.

<div id="syntax">
  ## Sintaxis
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## Parámetros
</div>

| Argumento        | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | Una dirección de servidor remoto o una expresión que genera varias direcciones de servidores remotos. Formato: `host` o `host:port`.<br /><br />    El `host` puede especificarse como nombre de servidor o como dirección IPv4 o IPv6. Una dirección IPv6 debe especificarse entre `[]`.<br /><br />    El `port` es el puerto TCP del servidor remoto. Si se omite el puerto, se usa [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) del archivo de configuración del servidor para la función de tabla `remote` (de manera predeterminada, 9000) y [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) para la función de tabla `remoteSecure` (de manera predeterminada, 9440).<br /><br />    Para direcciones IPv6, el puerto es obligatorio.<br /><br />    Si solo se especifica el parámetro `addresses_expr`, `db` y `table` usarán `system.one` de manera predeterminada.<br /><br />    Tipo: [String](../../sql-reference/data-types/string.md). |
| `db`             | Nombre de la base de datos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `table`          | Nombre de la tabla. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `user`           | Nombre de usuario. Si no se especifica, se usa `default`. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `password`       | Contraseña del usuario. Si no se especifica, se usa una contraseña vacía. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `sharding_key`   | Clave de segmentación para distribuir datos entre nodos. Por ejemplo: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Tipo: [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

Los argumentos también se pueden pasar mediante [colecciones con nombre](/es/operations/named-collections.md).

<div id="returned-value">
  ## Valor devuelto
</div>

Una tabla ubicada en un servidor remoto.

<div id="usage">
  ## Uso
</div>

Como las funciones de tabla `remote` y `remoteSecure` restablecen la conexión para cada petición, se recomienda usar en su lugar una tabla `Distributed`. Además, si se configuran nombres de host, los nombres se resuelven y los errores no se contabilizan al trabajar con varias réplicas. Al procesar un gran número de consultas, cree siempre la tabla `Distributed` con antelación y no utilice la función de tabla `remote`.

La función de tabla `remote` puede ser útil en los siguientes casos:

* Migración puntual de datos de un sistema a otro
* Acceso a un servidor específico para comparar datos, depurar y realizar pruebas; es decir, conexiones ad hoc.
* Consultas entre varios clústeres de ClickHouse con fines de investigación.
* Peticiones distribuidas poco frecuentes que se realizan manualmente.
* Peticiones distribuidas en las que el conjunto de servidores se redefine cada vez.

<div id="addresses">
  ### Direcciones
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Pueden indicarse varias direcciones separadas por comas. En ese caso, ClickHouse utilizará procesamiento distribuido y enviará la consulta a todas las direcciones especificadas (como si fueran segmentos con datos diferentes). Ejemplo:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## Ejemplos
</div>

<div id="selecting-data-from-a-remote-server">
  ### Selección de datos desde un servidor remoto:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

O bien usando [colecciones con nombre](/es/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### Insertar datos en una tabla de un servidor remoto:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### Migración de tablas de un sistema a otro:
</div>

Este ejemplo utiliza una tabla de un conjunto de datos de ejemplo. La base de datos es `imdb` y la tabla es `actors`.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### En el sistema ClickHouse de origen (el sistema que actualmente aloja los datos)
</div>

* Verifique la base de datos de origen y el nombre de la tabla (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* Obtenga la sentencia CREATE TABLE desde el origen:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

Respuesta

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### En el sistema ClickHouse de destino
</div>

* Cree la base de datos de destino:

  ```sql
  CREATE DATABASE imdb
  ```

* Con la sentencia CREATE TABLE del origen, cree la tabla de destino:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### De nuevo en la implementación de origen
</div>

Inserte datos en la nueva base de datos y la tabla creadas en el sistema remoto. Necesitará el host, el puerto, el nombre de usuario, la contraseña, la base de datos de destino y la tabla de destino.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## Globbing
</div>

Los patrones entre `{ }` se usan para generar un conjunto de segmentos y para especificar réplicas. Si hay varios pares de `{ }`, se genera el producto cartesiano de los conjuntos correspondientes.

Se admiten los siguientes tipos de patrones.

* `{a,b,c}` - Representa cualquiera de las cadenas alternativas `a`, `b` o `c`. El patrón se sustituye por `a` en la dirección del primer segmento, por `b` en la dirección del segundo segmento, y así sucesivamente. Por ejemplo, `example0{1,2}-1` genera las direcciones `example01-1` y `example02-1`.
* `{N..M}` - Un rango de números. Este patrón genera direcciones de segmentos con índices incrementales desde `N` hasta `M` (incluido). Por ejemplo, `example0{1..2}-1` genera `example01-1` y `example02-1`.
* `{0n..0m}` - Un rango de números con ceros a la izquierda. Este patrón conserva los ceros a la izquierda en los índices. Por ejemplo, `example{01..03}-1` genera `example01-1`, `example02-1` y `example03-1`.
* `{a|b}` - Cualquier cantidad de variantes separadas por `|`. El patrón especifica réplicas. Por ejemplo, `example01-{1|2}` genera las réplicas `example01-1` y `example01-2`.

La consulta se enviará a la primera réplica en buen estado. Sin embargo, para `remote`, las réplicas se recorren en el orden establecido actualmente en la configuración [load&#95;balancing](../../operations/settings/settings.md#load_balancing).
La cantidad de direcciones generadas está limitada por la configuración [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses).