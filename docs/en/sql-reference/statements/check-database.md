---
description: 'Documentation for CHECK DATABASE'
sidebar_label: 'CHECK DATABASE'
sidebar_position: 41
slug: /sql-reference/statements/check-database
title: 'CHECK DATABASE Statement'
doc_type: 'reference'
---

The `CHECK DATABASE` query verifies the health of a database.

Its primary use is with the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine, where it checks that the external catalog backing the database is reachable and that its list of tables can be retrieved. This is a lightweight probe: it confirms connectivity and authentication without reading any table data.

## Syntax {#syntax}

The basic syntax of the query is as follows:

```sql
CHECK DATABASE database_name
```

- `database_name`: Specifies the name of the database that you want to check.

The query does not return a result set. If the check succeeds, the query completes without producing any rows. If the check fails - for example, when the catalog cannot be reached or the credentials are no longer valid - the query throws an exception describing the failure.

## Behavior {#behavior}

For a database with the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) engine, `CHECK DATABASE`:

- Connects to the external catalog (for example, AWS Glue, Databricks Unity, Hive Metastore, or an Iceberg REST catalog) and retrieves its list of tables.
- Reports success if the catalog is reachable and authentication is valid.
- Does not require the catalog to contain any tables: an empty but reachable catalog is still considered healthy.

For database engines that do not implement a dedicated health check, `CHECK DATABASE` completes successfully without performing any work.

Executing `CHECK DATABASE` requires the `CHECK` privilege on the database (see [`GRANT`](/sql-reference/statements/grant)).

## Examples {#examples}

Check a `DataLakeCatalog` database named `datalake`:

```sql title="Query"
CHECK DATABASE datalake;
```

If the catalog is reachable and the credentials are valid, the query completes successfully and returns no rows.

If the catalog cannot be reached, the query throws an exception instead. The exact message depends on the catalog type and the underlying failure (for example, a connection error, an HTTP status such as `401 Unauthorized`, or a DNS resolution failure).

## See also {#see-also}

- [`CHECK TABLE`](/sql-reference/statements/check-table)
- [`DataLakeCatalog`](/engines/database-engines/datalakecatalog)
