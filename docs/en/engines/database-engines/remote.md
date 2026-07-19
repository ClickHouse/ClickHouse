---
description: 'Provides real-time access to the tables of a database on a remote ClickHouse server, forwarding `SELECT` and `INSERT` queries to it.'
sidebar_label: 'Remote'
sidebar_position: 51
slug: /engines/database-engines/remote
title: 'Remote'
doc_type: 'reference'
---

# Remote database engine {#remote-database-engine}

The `Remote` and `RemoteSecure` database engines provide real-time access to the tables of a database on a remote ClickHouse server over the native TCP protocol. They are the ClickHouse-to-ClickHouse counterparts of the [`MySQL`](/engines/database-engines/mysql) and [`PostgreSQL`](/engines/database-engines/postgresql) database engines.

The list of tables and their structure are fetched from the remote server on demand (using `SHOW TABLES` and `DESCRIBE TABLE` under the hood), so the database always reflects the current state of the remote server. Each table is exposed as a [`Distributed`](/engines/table-engines/special/distributed) storage over an ad-hoc cluster built from the supplied addresses, which forwards `SELECT` and `INSERT` queries to the remote server.

This is handy for federating several ClickHouse clusters or for plugging a larger ClickHouse cluster into `clickhouse-local` or a smaller cluster.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE remote_db
ENGINE = Remote('addresses_expr', 'database'[, 'user'[, 'password']]);
```

Use `RemoteSecure` to connect over a secure TLS connection:

```sql
CREATE DATABASE remote_db
ENGINE = RemoteSecure('addresses_expr', 'database'[, 'user'[, 'password']]);
```

**Engine Parameters**

- `addresses_expr` — A remote server address or an expression that generates several addresses, in the form `host` or `host:port`. The address expression supports the same globbing patterns as the [`remote`](/sql-reference/table-functions/remote) table function (for example `{a,b,c}`, `{N..M}` and `{a|b}` to expand into multiple shards and replicas). When the port is omitted, `Remote` uses the plain TCP port (`tcp_port`, `9000` by default) and `RemoteSecure` uses the secure TCP port (`tcp_port_secure`, `9440` by default).
- `database` — The name of the database on the remote server.
- `user` — The remote user name. Optional, default: `default`.
- `password` — The remote user password. Optional, default: empty.

The addresses and credentials are stored in the database definition, so the password is hidden in `SHOW CREATE DATABASE`. As with the `remote` table function, an address that points to the current server is treated as a local shard: `SELECT` and `INSERT` are executed directly under the current user and the stored credentials are used only for genuinely remote servers.

Named collections are supported as well:

```sql
CREATE DATABASE remote_db
ENGINE = Remote(my_named_collection, database = 'default');
```

## Notes {#notes}

- The engine is a read-through view of the remote server: `CREATE TABLE`, `DROP TABLE`, `ALTER` and similar DDL statements against the `Remote` database are not supported. Manage the schema on the remote server directly.
- Access rights are enforced on the remote server for the configured remote user, and locally by the usual privileges on the database and its tables.

## Example {#example}

Create a `Remote` database that points to the `system` database of a remote server and read from it:

```sql
CREATE DATABASE remote_system
ENGINE = Remote('127.0.0.1:9000', 'system', 'default', '');
```

```sql
SHOW TABLES FROM remote_system LIKE 'one';
```

```text
┌─name─┐
│ one  │
└──────┘
```

```sql
SELECT * FROM remote_system.one;
```

```text
┌─dummy─┐
│     0 │
└───────┘
```
