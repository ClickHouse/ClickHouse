---
description: 'Permite ejecutar consultas `SELECT` e `INSERT` sobre datos almacenados
  en un servidor PostgreSQL remoto.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

Permite ejecutar consultas `SELECT` e `INSERT` sobre datos almacenados en un servidor PostgreSQL remoto.

<div id="syntax">
  ## Sintaxis
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento     | Descripción                                                                                                                                               |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | Dirección del servidor PostgreSQL.                                                                                                                        |
| `database`    | Nombre de la base de datos remota.                                                                                                                        |
| `table`       | Nombre de la tabla remota, o una consulta enviada a PostgreSQL tal cual (consulte [Pasar una consulta en lugar de un nombre de tabla](#passing-a-query)). |
| `user`        | Usuario de PostgreSQL.                                                                                                                                    |
| `password`    | Contraseña del usuario.                                                                                                                                   |
| `schema`      | Esquema de tabla distinto del predeterminado. Opcional.                                                                                                   |
| `on_conflict` | Estrategia de resolución de conflictos. Ejemplo: `ON CONFLICT DO NOTHING`. Opcional.                                                                      |

Los argumentos también pueden pasarse mediante [colecciones con nombre](/es/operations/named-collections.md). En este caso, `host` y `port` deben especificarse por separado. Este enfoque se recomienda para entornos de producción.

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tabla con las mismas columnas que la tabla original de PostgreSQL.

:::note
En la consulta `INSERT`, para distinguir la función de tabla `postgresql(...)` del nombre de la tabla con la lista de nombres de columnas, debe usar las palabras clave `FUNCTION` o `TABLE FUNCTION`. Consulte los ejemplos a continuación.
:::

<div id="implementation-details">
  ## Detalles de implementación
</div>

Las consultas `SELECT` en PostgreSQL se ejecutan como `COPY (SELECT ...) TO STDOUT` dentro de una transacción de PostgreSQL de solo lectura, con commit después de cada consulta `SELECT`.

Las cláusulas `WHERE` simples, como `=`, `!=`, `>`, `>=`, `<`, `<=` e `IN`, se ejecutan en el servidor PostgreSQL.

Todos los joins, las agregaciones, la ordenación, las condiciones `IN [ array ]` y la restricción de sampling `LIMIT` se ejecutan en ClickHouse solo una vez finalizada la consulta a PostgreSQL.

<div id="passing-a-query">
  ## Pasar una consulta en lugar del nombre de una tabla
</div>

En lugar del nombre de una tabla, el tercer argumento puede ser una consulta `SELECT` que se pasa a PostgreSQL tal cual. La estructura de la tabla resultante se infiere a partir del resultado de la consulta. La consulta puede escribirse como una subconsulta o envolverse en la función `query`:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Esto es útil para hacer pushdown de joins, agregaciones o cualquier otro procesamiento a PostgreSQL. Dicha tabla es de solo lectura: no se permite `INSERT` en ella. La misma sintaxis es compatible con el motor de tabla [`PostgreSQL`](/es/engines/table-engines/integrations/postgresql).

:::note
La forma de subconsulta `(SELECT ...)` es analizada por ClickHouse y serializada de nuevo en el dialecto de PostgreSQL (entrecomillado de identificadores de PostgreSQL y escape de literales de cadena) antes de enviarse al servidor. Por lo tanto, debe ser válida en ClickHouse SQL. Para pasar sintaxis específica de PostgreSQL que ClickHouse no analiza, use la forma `query('...')`, cuyo texto se envía a PostgreSQL literalmente.

Cualquier `WHERE`, `LIMIT`, agregación, etc. externo de la consulta de ClickHouse circundante **no** se hace pushdown en la consulta proporcionada; se aplica en ClickHouse después de obtener el resultado completo de la consulta. Para restringir los datos leídos desde PostgreSQL, coloque el filtro dentro de la consulta proporcionada. Con [`external_table_strict_query = 1`](/es/operations/settings/settings#external_table_strict_query), un filtro externo que no puede hacerse pushdown se rechaza con una excepción en lugar de aplicarse localmente.
:::

Las consultas `INSERT` del lado de PostgreSQL se ejecutan como `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` dentro de una transacción de PostgreSQL con commit automático después de cada sentencia `INSERT`.

Los tipos Array de PostgreSQL se convierten en arrays de ClickHouse.

:::note
Tenga cuidado: en PostgreSQL, una columna de tipo array como Integer[] puede contener arrays de distintas dimensiones en diferentes filas, pero en ClickHouse solo se permite tener arrays multidimensionales de la misma dimensión en todas las filas.
:::

Admite múltiples réplicas que deben enumerarse mediante `|`. Por ejemplo:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Admite la prioridad de las réplicas para la fuente de diccionario de PostgreSQL. Cuanto mayor sea el número en `map`, menor será la prioridad. La prioridad más alta es `0`.

<div id="examples">
  ## Ejemplos
</div>

Tabla en PostgreSQL:

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

Selección de datos de ClickHouse con argumentos simples:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

O bien usando [colecciones con nombre](/es/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Inserción:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Uso de un esquema distinto del predeterminado:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## Relacionado
</div>

* [El motor de tabla de PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
* [Uso de PostgreSQL como fuente de diccionario](/es/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### Replicar o migrar datos de Postgres con PeerDB
</div>

> Además de las funciones de tabla, también puedes usar [PeerDB](https://docs.peerdb.io/introduction) de ClickHouse para configurar un pipeline de datos continuo de Postgres a ClickHouse. PeerDB es una herramienta diseñada específicamente para replicar datos de Postgres a ClickHouse mediante captura de datos modificados (CDC).