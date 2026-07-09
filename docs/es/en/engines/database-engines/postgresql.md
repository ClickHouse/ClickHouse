---
description: 'Permite conectarse a bases de datos en un servidor PostgreSQL remoto.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

Permite conectarse a bases de datos en un servidor [PostgreSQL](https://www.postgresql.org) remoto. Admite operaciones de lectura y escritura (consultas `SELECT` e `INSERT`) para intercambiar datos entre ClickHouse y PostgreSQL.

Proporciona acceso en tiempo real a la lista de tablas y a la estructura de las tablas del servidor PostgreSQL remoto mediante las consultas `SHOW TABLES` y `DESCRIBE TABLE`.

Admite modificaciones de la estructura de las tablas (`ALTER TABLE ... ADD|DROP COLUMN`). Si el parámetro `use_table_cache` (consulte los parámetros del motor a continuación) está establecido en `1`, la estructura de la tabla se almacena en caché y no se verifica si se ha modificado, pero puede actualizarse con las consultas `DETACH` y `ATTACH`.

<div id="creating-a-database">
  ## Creación de una base de datos
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**Parámetros del motor**

* `host:port` — Dirección del servidor PostgreSQL.
* `database` — Nombre de la base de datos remota.
* `user` — Usuario de PostgreSQL.
* `password` — Contraseña del usuario.
* `schema` — Esquema de PostgreSQL.
* `use_table_cache` — Define si la estructura de la tabla de la base de datos se almacena en caché o no. Opcional. Valor predeterminado: `0`.

<div id="data_types-support">
  ## Compatibilidad con tipos de datos
</div>

| PostgreSQL       | ClickHouse                                                                     |
| ---------------- | ------------------------------------------------------------------------------ |
| DATE             | [Date](../../sql-reference/data-types/date.md)                                 |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)                         |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                             |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                             |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (véase la nota siguiente) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                            |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                            |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                            |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)                           |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)                           |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                             |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))                  |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                               |

:::note
PostgreSQL `numeric(p, 0)` con una precisión `p` superior a 76 (el máximo admitido por `Decimal256`) —por ejemplo, `numeric(78, 0)`, que se usa habitualmente para almacenar enteros de 256 bits— se asigna a [`Int256`](../../sql-reference/data-types/int-uint.md) en lugar de `Decimal`. Los valores que no caben en el rango de `Int256` se rechazan con un error.
:::

<div id="examples-of-use">
  ## Ejemplos de uso
</div>

Base de datos en ClickHouse que intercambia datos con el servidor PostgreSQL:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

Lectura de datos desde la tabla de PostgreSQL:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Escribir datos en la tabla de PostgreSQL:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

Supongamos que se modificó la estructura de la tabla en PostgreSQL:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

Como el parámetro `use_table_cache` se estableció en `1` cuando se creó la base de datos, la estructura de la tabla en ClickHouse quedó almacenada en caché y, por lo tanto, no se modificó:

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

Después de desvincular la tabla y volver a vincularla, la estructura se actualizó:

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [ClickHouse y PostgreSQL: una pareja hecha en el paraíso de los datos - parte 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Blog: [ClickHouse y PostgreSQL: una pareja hecha en el paraíso de los datos - parte 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)