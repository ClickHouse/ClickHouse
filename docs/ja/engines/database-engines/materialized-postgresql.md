---
slug: /ja/engines/database-engines/materialized-postgresql
sidebar_label: MaterializedPostgreSQL
sidebar_position: 60
---

# [experimental] MaterializedPostgreSQL

PostgreSQL データベースからテーブルを ClickHouse データベースとして作成します。最初に、`MaterializedPostgreSQL` エンジンを使って PostgreSQL データベースのスナップショットを作成し、必要なテーブルをロードします。必要なテーブルには、指定されたデータベース内の任意のスキーマの任意のサブセットのテーブルを含めることができます。スナップショットの取得と同時に、データベースエンジンは LSN を取得し、テーブルの初期ダンプが行われると、WAL から更新をプルし始めます。データベースが作成されると、PostgreSQL データベースに新しく追加されたテーブルは自動的にレプリケーションに追加されません。それらは `ATTACH TABLE db.table` クエリを使用して手動で追加する必要があります。

レプリケーションは PostgreSQL の論理レプリケーションプロトコルで実装されています。これにより、DDL のレプリケートはできませんが、レプリケーションを壊す変更（カラム型の変更、カラムの追加/削除）が発生したかどうかを知ることができます。このような変更が検出されると、該当するテーブルの更新受信が停止します。この場合、テーブルを完全に再読み込みするには `ATTACH`/`DETACH PERMANENTLY` クエリを使用する必要があります。もし DDL がレプリケーションを壊さなければ（例えば、カラムの名前変更）、テーブルは更新を受信し続けます（挿入は位置で行われます）。

:::note
このデータベースエンジンはエクスペリメンタルです。使用するには、構成ファイルで `allow_experimental_database_materialized_postgresql` を 1 に設定するか、`SET` コマンドを使用します:
```sql
SET allow_experimental_database_materialized_postgresql=1
```
:::

## データベースの作成 {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**エンジンパラメータ**

- `host:port` — PostgreSQL サーバーエンドポイント。
- `database` — PostgreSQL データベース名。
- `user` — PostgreSQL ユーザー。
- `password` — ユーザーパスワード。

## 使用例 {#example-of-use}

``` sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgresql_db.postgres_table;
```

## 新しいテーブルを動的にレプリケーションに追加 {#dynamically-adding-table-to-replication}

`MaterializedPostgreSQL` データベースが作成された後、対応する PostgreSQL データベースの新しいテーブルは自動的に検出されません。これらのテーブルは手動で追加できます:

``` sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
バージョン 22.1 以前では、レプリケーションにテーブルを追加すると、削除されない一時的なレプリケーションスロット（`{db_name}_ch_replication_slot_tmp` と命名）が残ります。22.1 より前の ClickHouse バージョンでテーブルをアタッチする場合は、手動で削除することを確認してください（`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`）。さもないと、ディスク使用量が増加します。この問題は 22.1 で修正されました。
:::

## レプリケーションからのテーブルを動的に削除 {#dynamically-removing-table-from-replication}

特定のテーブルをレプリケーションから削除することができます:

``` sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

## PostgreSQL スキーマ {#schema}

PostgreSQL [スキーマ](https://www.postgresql.org/docs/9.1/ddl-schemas.html) は 3 つの方法で設定できます（バージョン 21.12 以降）。

1. `MaterializedPostgreSQL` データベースエンジンに対して 1 つのスキーマ。設定 `materialized_postgresql_schema` を使用する必要があります。
テーブルへのアクセスはテーブル名のみを介します:

``` sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. 任意の数のスキーマと指定されたテーブルセットを持つ `MaterializedPostgreSQL` データベースエンジン。設定 `materialized_postgresql_tables_list` を使用する必要があります。それぞれのテーブルはスキーマと共に書かれます。
テーブルへのアクセスはスキーマ名とテーブル名を同時に指定して行われます:

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

しかし、この場合、`materialized_postgresql_tables_list` にあるすべてのテーブルはスキーマ名と一緒に書かれる必要があります。
`materialized_postgresql_tables_list_with_schema = 1` が必要です。

注意: この場合、テーブル名にドットは許可されません。

3. 任意の数のスキーマを持ち、全テーブルを設定する `MaterializedPostgreSQL` データベースエンジン。設定 `materialized_postgresql_schema_list` を使用する必要があります。

``` sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

注意: この場合、テーブル名にドットは許可されません。

## 必要条件 {#requirements}

1. [wal_level](https://www.postgresql.org/docs/current/runtime-config-wal.html) 設定は `logical` の値を持ち、`max_replication_slots` パラメータは PostgreSQL 設定ファイルで少なくとも `2` の値を持つ必要があります。

2. それぞれのレプリケートされたテーブルは次のいずれかの [レプリカアイデンティティ](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) を持つ必要があります:

- 主キー（デフォルト）

- インデックス

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

主キーは常に最初にチェックされます。存在しない場合、レプリカアイデンティティインデックスとして定義されたインデックスがチェックされます。インデックスはレプリカアイデンティティとして使用される場合、テーブル内に一つだけ存在する必要があります。
特定のテーブルでどのタイプが使用されるかを確認するには、次のコマンドを使用します:

``` bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
[**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) 値のレプリケーションはサポートされていません。デフォルトではデータ型のデフォルト値が使用されます。
:::

## 設定 {#settings}

### `materialized_postgresql_tables_list` {#materialized-postgresql-tables-list}

    PostgreSQL データベーステーブルのカンマ区切りリストを設定します。これらのテーブルは [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md) データベースエンジンを介してレプリケートされます。

    各テーブルは、括弧内にレプリケートされたカラムのサブセットを持つことができます。カラムのサブセットが省略された場合、テーブルのすべてのカラムがレプリケートされます。

    ``` sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
    ```

    デフォルト値: 空のリスト — PostgreSQL データベース全体がレプリケートされることを意味します。

### `materialized_postgresql_schema` {#materialized-postgresql-schema}

    デフォルト値: 空の文字列。（デフォルトスキーマが使用されます）

### `materialized_postgresql_schema_list` {#materialized-postgresql-schema-list}

    デフォルト値: 空のリスト。（デフォルトスキーマが使用されます）

### `materialized_postgresql_max_block_size` {#materialized-postgresql-max-block-size}

    PostgreSQL データベーステーブルにデータを書き込む前にメモリに収集される行数を設定します。

    可能な値:

    - 正の整数。

    デフォルト値: `65536`.

### `materialized_postgresql_replication_slot` {#materialized-postgresql-replication-slot}

    ユーザーが作成したレプリケーションスロット。`materialized_postgresql_snapshot` と一緒に使用する必要があります。

### `materialized_postgresql_snapshot` {#materialized-postgresql-snapshot}

    文字列として識別されるスナップショット。これにより、[PostgreSQL テーブルの初期ダンプ](../../engines/database-engines/materialized-postgresql.md)が実行されます。`materialized_postgresql_replication_slot` と一緒に使用する必要があります。

    ``` sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
    ```

    設定は DDL クエリを使用して必要に応じて変更できます。ただし、`materialized_postgresql_tables_list` 設定は変更できません。この設定でテーブルのリストを更新するには、`ATTACH TABLE` クエリを使用してください。

    ``` sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
    ```

### `materialized_postgresql_use_unique_replication_consumer_identifier` {#materialized-postgresql-use-unique-replication-consumer-identifier}

レプリケーションのためにユニークなレプリケーションコンシューマ識別子を使用します。デフォルト: `0`。
もし `1` に設定すると、同じ `PostgreSQL` テーブルを指し示す複数の `MaterializedPostgreSQL` テーブルを設定することができます。

## 注意事項 {#notes}

### 論理レプリケーションスロットのフェイルオーバー {#logical-replication-slot-failover}

プライマリに存在する論理レプリケーションスロットは、スタンバイレプリカでは利用できません。
したがって、フェイルオーバーが発生した場合、新しいプライマリ（古い物理スタンバイ）は、古いプライマリで存在していたスロットを認識しません。これは PostgreSQL からのレプリケーションを壊すことになります。
これを解決する方法の一つは、レプリケーションスロットを自分で管理し、永続的なレプリケーションスロットを定義することです（いくつかの情報は[こちら](https://patroni.readthedocs.io/en/latest/SETTINGS.html)で見つけることができます）。`materialized_postgresql_replication_slot` 設定を通じてスロット名を渡す必要があり、`EXPORT SNAPSHOT` オプションでエクスポートされる必要があります。スナップショット識別子は `materialized_postgresql_snapshot` 設定を通じて渡す必要があります。

注意すべきは、本当に必要な場合のみこれを使用すべきであるということです。これを行う具体的な必要性や完全な理解がない限り、テーブルエンジンが独自にレプリケーションスロットを作成し管理することを許可した方が良いです。

**例（[@bchrobot](https://github.com/bchrobot) より）**

1. PostgreSQL でレプリケーションスロットを設定します。

    ```yaml
    apiVersion: "acid.zalan.do/v1"
    kind: postgresql
    metadata:
      name: acid-demo-cluster
    spec:
      numberOfInstances: 2
      postgresql:
        parameters:
          wal_level: logical
      patroni:
        slots:
          clickhouse_sync:
            type: logical
            database: demodb
            plugin: pgoutput
    ```

2. レプリケーションスロットが準備されるのを待ち、トランザクションを開始し、トランザクションスナップショット識別子をエクスポートします:

    ```sql
    BEGIN;
    SELECT pg_export_snapshot();
    ```

3. ClickHouse でデータベースを作成します:

    ```sql
    CREATE DATABASE demodb
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS
      materialized_postgresql_replication_slot = 'clickhouse_sync',
      materialized_postgresql_snapshot = '0000000A-0000023F-3',
      materialized_postgresql_tables_list = 'table1,table2,table3';
    ```

4. PostgreSQL トランザクションを終了したら、ClickHouse DB へのレプリケーションが確認されます。フェイルオーバー後もレプリケーションが続くことを確認します:

    ```bash
    kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
    ```

### 必要な権限

1. [CREATE PUBLICATION](https://postgrespro.ru/docs/postgresql/14/sql-createpublication) -- 作成クエリ権限。

2. [CREATE_REPLICATION_SLOT](https://postgrespro.ru/docs/postgrespro/10/protocol-replication#PROTOCOL-REPLICATION-CREATE-SLOT) -- レプリケーション権限。

3. [pg_drop_replication_slot](https://postgrespro.ru/docs/postgrespro/9.5/functions-admin#functions-replication) -- レプリケーション権限またはスーパーユーザー。

4. [DROP PUBLICATION](https://postgrespro.ru/docs/postgresql/10/sql-droppublication) -- パブリケーションの所有者（`MaterializedPostgreSQL` エンジン内の `username`）。

`materialized_postgresql_replication_slot` と `materialized_postgresql_snapshot` 設定を使用することで、コマンド `2` および `3` の実行とその権限を避けることが可能です。ただし、非常に注意が必要です。

テーブルへのアクセス権:

1. pg_publication

2. pg_replication_slots

3. pg_publication_tables
