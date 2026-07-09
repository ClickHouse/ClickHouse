---
description: 'Devuelve una tabla conectada mediante un driver JDBC.'
sidebar_label: 'jdbc'
sidebar_position: 100
slug: /sql-reference/table-functions/jdbc
title: 'jdbc'
doc_type: 'reference'
---

:::note
clickhouse-jdbc-bridge contiene código experimental y ya no tiene soporte. Puede presentar problemas de fiabilidad y vulnerabilidades de seguridad. Úselo bajo su propia responsabilidad.
ClickHouse recomienda usar las funciones de tabla integradas de ClickHouse, que ofrecen una mejor alternativa para consultas ad hoc (Postgres, MySQL, MongoDB, etc.).
:::

La función de tabla JDBC devuelve una tabla conectada mediante un driver JDBC.

Esta función de tabla requiere que el programa [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) se ejecute por separado.
Admite tipos Nullable (según el DDL de la tabla remota consultada).

<div id="syntax">
  ## Sintaxis
</div>

```sql
jdbc(datasource, external_database, external_table)
jdbc(datasource, external_table)
jdbc(named_collection)
```

<div id="examples">
  ## Ejemplos
</div>

En lugar del nombre de una base de datos externa, se puede especificar un esquema:

```sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

```sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'select * from schema.table')
```

```sql
SELECT * FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT *
FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

```sql
SELECT a.datasource AS server1, b.datasource AS server2, b.name AS db
FROM jdbc('mysql-dev?datasource_column', 'show databases') a
INNER JOIN jdbc('self?datasource_column', 'show databases') b ON a.Database = b.name
```