---
slug: /ja/sql-reference/statements/create/database
sidebar_position: 35
sidebar_label: DATABASE
---

# CREATE DATABASE

新しいデータベースを作成します。

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'Comment']
```

## 句

### IF NOT EXISTS

`db_name` データベースが既に存在する場合、ClickHouseは新しいデータベースを作成せずに以下のように動作します。

- この句が指定されている場合、例外を投げません。
- この句が指定されていない場合、例外を投げます。

### ON CLUSTER

ClickHouseは、指定されたクラスタ内のすべてのサーバー上に `db_name` データベースを作成します。詳細は[分散DDL](../../../sql-reference/distributed-ddl.md)の記事を参照してください。

### ENGINE

デフォルトでは、ClickHouseは自身の[Atomic](../../../engines/database-engines/atomic.md)データベースエンジンを使用します。他にも[Lazy](../../../engines/database-engines/lazy.md)、[MySQL](../../../engines/database-engines/mysql.md)、[PostgresSQL](../../../engines/database-engines/postgresql.md)、[MaterializedMySQL](../../../engines/database-engines/materialized-mysql.md)、[MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md)、[Replicated](../../../engines/database-engines/replicated.md)、[SQLite](../../../engines/database-engines/sqlite.md)があります。

### COMMENT

データベースを作成するときにコメントを追加することができます。

コメントはすべてのデータベースエンジンでサポートされています。

**構文**

``` sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**例**

クエリ:

``` sql
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

結果:

```text
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```
