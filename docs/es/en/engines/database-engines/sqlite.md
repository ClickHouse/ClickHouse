---
description: 'Permite conectarse a bases de datos SQLite y ejecutar consultas `INSERT` y `SELECT`
  para intercambiar datos entre ClickHouse y SQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

Permite conectarse a una base de datos [SQLite](https://www.sqlite.org/index.html) y ejecutar consultas `INSERT` y `SELECT` para intercambiar datos entre ClickHouse y SQLite.

<div id="creating-a-database">
  ## Crear una base de datos
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Parámetros del motor**

* `db_path` — Ruta del archivo con la base de datos SQLite.

<div id="data_types-support">
  ## Compatibilidad de tipos de datos
</div>

La tabla siguiente muestra la correspondencia de tipos predeterminada cuando ClickHouse infiere automáticamente el esquema a partir de SQLite:

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

Cuando define explícitamente una tabla con tipos específicos de ClickHouse usando el [motor de tabla SQLite](../../engines/table-engines/integrations/sqlite.md), los siguientes tipos de ClickHouse pueden analizarse a partir de columnas TEXT de SQLite:

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* Todos los tipos enteros ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite tiene tipado dinámico, y sus funciones de acceso tipado realizan coerción automática de tipos. Por ejemplo, leer una columna TEXT como un entero devolverá 0 si el texto no puede analizarse como un número. Esto significa que, si una tabla de ClickHouse se define con un tipo distinto del tipo real de la columna SQLite subyacente, los valores pueden convertirse silenciosamente en lugar de provocar un error.

<div id="specifics-and-recommendations">
  ## Aspectos específicos y recomendaciones
</div>

SQLite almacena toda la base de datos (definiciones, tablas, índices y los propios datos) en un único archivo multiplataforma en la máquina anfitriona. Durante la escritura, SQLite bloquea todo el archivo de la base de datos; por lo tanto, las operaciones de escritura se realizan secuencialmente. Las operaciones de lectura pueden ejecutarse de forma concurrente.
SQLite no requiere administración de servicios (como scripts de inicio) ni control de acceso basado en `GRANT` y contraseñas. El control de acceso se gestiona mediante los permisos del sistema de archivos otorgados al propio archivo de la base de datos.

<div id="usage-example">
  ## Ejemplo de uso
</div>

Base de datos en ClickHouse conectada a SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Muestra las tablas:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

Inserción de datos en una tabla de SQLite desde una tabla de ClickHouse:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```