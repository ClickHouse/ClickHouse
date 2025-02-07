---
slug: /ja/sql-reference/statements/show
sidebar_position: 37
sidebar_label: SHOW
---

# SHOW ステートメント

N.B. `SHOW CREATE (TABLE|DATABASE|USER)` は、[`display_secrets_in_show_and_select` サーバー設定](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)が有効になっておらず、[`format_display_secrets_in_show_and_select` フォーマット設定](../../operations/settings/formats#format_display_secrets_in_show_and_select)が有効になっておらず、またユーザーに[`displaySecretsInShowAndSelect`](grant.md#display-secrets) 権限がない限り、秘密情報を隠すようにします。

## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE

``` sql
SHOW [CREATE] [TEMPORARY] TABLE|DICTIONARY|VIEW|DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

指定されたオブジェクトを作成するために使用されたCREATEクエリを含む、String型の単一のカラムを返します。

`SHOW TABLE t` および `SHOW DATABASE db` は `SHOW CREATE TABLE|DATABASE t|db` と同じ意味を持ちますが、`SHOW t` および `SHOW db` はサポートされていません。

システムテーブルの `CREATE` クエリを取得するためにこのステートメントを使用すると、テーブル構造のみを宣言する *偽の* クエリが返され、テーブルを作成するために使用することはできません。

## SHOW DATABASES

すべてのデータベースの一覧を表示します。

```sql
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

このステートメントは、次のクエリと同一です：

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

**例**

名前に 'de' というシーケンスを含むデータベース名を取得する：

``` sql
SHOW DATABASES LIKE '%de%'
```

結果:

``` text
┌─name────┐
│ default │
└─────────┘
```

大文字小文字を区別しない方法で、名前に 'de' というシーケンスを含むデータベース名を取得する：

``` sql
SHOW DATABASES ILIKE '%DE%'
```

結果:

``` text
┌─name────┐
│ default │
└─────────┘
```

名前に 'de' というシーケンスを含まないデータベース名を取得する：

``` sql
SHOW DATABASES NOT LIKE '%de%'
```

結果:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

データベース名から最初の二行を取得する：

``` sql
SHOW DATABASES LIMIT 2
```

結果:

``` text
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

**参考**
- [CREATE DATABASE](https://clickhouse.com/docs/ja/sql-reference/statements/create/database/#query-language-create-database)

## SHOW TABLES

テーブルのリストを表示します。

```sql
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

`FROM` 句が指定されていない場合、クエリは現在のデータベースのテーブルリストを返します。

このステートメントは、次のクエリと同一です：

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例**

名前に 'user' というシーケンスを含むテーブル名を取得する：

``` sql
SHOW TABLES FROM system LIKE '%user%'
```

結果:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

大文字小文字を区別しない方法で、名前に 'user' を含むテーブル名を取得する：

``` sql
SHOW TABLES FROM system ILIKE '%USER%'
```

結果:

``` text
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

名前に 's' というシンボルシーケンスを含まないテーブル名を取得する：

``` sql
SHOW TABLES FROM system NOT LIKE '%s%'
```

結果:

``` text
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

テーブル名から最初の二行を取得する：

``` sql
SHOW TABLES FROM system LIMIT 2
```

結果:

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

**参考**

- [Create Tables](https://clickhouse.com/docs/ja/getting-started/tutorial/#create-tables)
- [SHOW CREATE TABLE](https://clickhouse.com/docs/ja/sql-reference/statements/show/#show-create-table)

## SHOW COLUMNS {#show_columns}

カラムのリストを表示します

```sql
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

データベース名とテーブル名は、`<db>.<table>` の省略形で指定でき、`FROM tab FROM db` および `FROM db.tab` は等価です。データベースが指定されていない場合、クエリは現在のデータベースからカラムのリストを返します。

オプションのキーワード `EXTENDED` は現在、効果はありません。これはMySQLとの互換性のために存在します。

オプションのキーワード `FULL` は、出力に照合順序、コメント、特権カラムを含めるようにします。

このステートメントは以下の構造を持つ結果テーブルを生成します：
- `field` - カラムの名前 (String)
- `type` - カラムデータ型。クエリがMySQLワイヤープロトコルを通じて行われた場合、MySQLでの同等の型名が表示されます。 (String)
- `null` - カラムデータ型がNullableの場合は `YES`、それ以外は `NO` (String)
- `key` - カラムが主キーの一部の場合は `PRI`、ソートキーの一部の場合は `SOR`、その他の場合は空 (String)
- `default` - カラムが `ALIAS`、`DEFAULT`、または `MATERIALIZED` 型の場合のデフォルト式。他は `NULL`。 (Nullable(String))
- `extra` - 追加情報、現在は未使用 (String)
- `collation` - (`FULL` キーワードを指定した場合のみ) カラムの照合順序、ClickHouseにはカラムごとの照合順序がないため常に `NULL` (Nullable(String))
- `comment` - (`FULL` キーワードを指定した場合のみ) カラムのコメント (String)
- `privilege` - (`FULL` キーワードを指定した場合のみ) このカラムに対する特権、現在は利用できません (String)

**例**

'order' テーブル内の 'delivery_' で始まるすべてのカラムの情報を取得：

```sql
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

結果:

``` text
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

**参考**
- [system.columns](https://clickhouse.com/docs/ja/operations/system-tables/columns)

## SHOW DICTIONARIES

[Dictionary](../../sql-reference/dictionaries/index.md) のリストを表示します。

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

`FROM` 句が指定されていない場合、クエリは現在のデータベースからのDictionaryのリストを返します。

`SHOW DICTIONARIES` クエリと同じ結果を次の方法で取得できます：

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**例**

次のクエリは、名前に `reg` を含む、`system` データベースのテーブルから最初の2行を選択します。

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW INDEX

テーブルの主キー及びデータスキッピングインデックスのリストを表示します。

このステートメントは主にMySQLとの互換性のために存在します。システムテーブル [system.tables](../../operations/system-tables/tables.md) (主キー用) および [system.data_skipping_indices](../../operations/system-tables/data_skipping_indices.md) (データスキッピングインデックス用) は、ClickHouseによりネイティブな形で同等の情報を提供します。

```sql
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

データベースおよびテーブル名は、`<db>.<table>` の省略形で指定でき、`FROM tab FROM db` および `FROM db.tab` は等価です。データベースが指定されていない場合、クエリは現在のデータベースをデータベースとして想定します。

オプションのキーワード `EXTENDED` は現在、効果はありません。これはMySQLとの互換性のために存在します。

このステートメントは以下の構造を持つ結果テーブルを生成します：
- `table` - テーブル名。(String)
- `non_unique` - ClickHouseは一意性制約をサポートしていないため、常に `1`。(UInt8)
- `key_name` - インデックス名、主キーインデックスの場合は `PRIMARY`。(String)
- `seq_in_index` - 主キーインデックスの場合、カラムの位置は `1` から始まります。データスキッピングインデックスの場合：常に `1`。(UInt8)
- `column_name` - 主キーインデックスの場合、カラム名。データスキッピングインデックスの場合：`''` (空文字列)、フィールド "expression" を参照。(String)
- `collation` - インデックス内のカラムのソート順序：昇順の場合は `A`、降順の場合は `D`、ソートされていない場合は `NULL`。(Nullable(String))
- `cardinality` - インデックスのカーディナリティ（インデックス内の一意の値の数）の推定値。現在は常に 0。(UInt64)
- `sub_part` - ClickHouseはMySQLのようなインデックスのプレフィックスをサポートしていないため、常に `NULL`。(Nullable(String))
- `packed` - ClickHouseはMySQLのようなパックドインデックスをサポートしていないため、常に `NULL`。(Nullable(String))
- `null` - 現在未使用
- `index_type` - インデックスタイプ、例：`PRIMARY`、`MINMAX`、`BLOOM_FILTER` など。(String)
- `comment` - 現在常に `''`（空文字列）であるインデックスに関する追加情報。(String)
- `index_comment` - ClickHouseではインデックスに `COMMENT` フィールド（MySQLのような）を持たせることができないため、`''`（空文字列）。(String)
- `visible` - インデックスがオプティマイザに見える場合、常に `YES`。(String)
- `expression` - データスキッピングインデックスの場合、インデックス式。主キーインデックスの場合：`''`（空文字列）。(String)

**例**

'tbl' テーブル内のすべてのインデックスの情報を取得：

```sql
SHOW INDEX FROM 'tbl'
```

結果:

``` text
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

**参考**
- [system.tables](../../operations/system-tables/tables.md)
- [system.data_skipping_indices](../../operations/system-tables/data_skipping_indices.md)

## SHOW PROCESSLIST

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

[system.processes](../../operations/system-tables/processes.md#system_tables-processes) テーブルの内容を出力します。このテーブルには、現在処理中のクエリのリストが含まれており、`SHOW PROCESSLIST` クエリは除外されます。

`SELECT * FROM system.processes` クエリは、すべての現在のクエリに関するデータを返します。

Tip (コンソールで実行):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW GRANTS

ユーザーの特権を表示します。

**構文**

``` sql
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

ユーザーが指定されていない場合、クエリは現在のユーザーの特権を返します。

`WITH IMPLICIT` 修飾子は、暗黙の権限を表示することを可能にします（例：`GRANT SELECT ON system.one`）

`FINAL` 修飾子は、ユーザーとその付与された役割（継承付き）からすべての権限をマージします。

## SHOW CREATE USER

[ユーザー作成](../../sql-reference/statements/create/user.md)で使用されたパラメーターを表示します。

**構文**

``` sql
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

## SHOW CREATE ROLE

[ロール作成](../../sql-reference/statements/create/role.md)で使用されたパラメーターを表示します。

**構文**

``` sql
SHOW CREATE ROLE name1 [, name2 ...]
```

## SHOW CREATE ROW POLICY

[ローポリシー作成](../../sql-reference/statements/create/row-policy.md)で使用されたパラメーターを表示します。

**構文**

``` sql
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

## SHOW CREATE QUOTA

[クォータ作成](../../sql-reference/statements/create/quota.md)で使用されたパラメーターを表示します。

**構文**

``` sql
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE

[設定プロファイル作成](../../sql-reference/statements/create/settings-profile.md)で使用されたパラメーターを表示します。

**構文**

``` sql
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

## SHOW USERS

[ユーザーアカウント](../../guides/sre/user-management/index.md#user-account-management)の名前を返します。ユーザーアカウントのパラメーターを表示するには、システムテーブル [system.users](../../operations/system-tables/users.md#system_tables-users) を参照してください。

**構文**

``` sql
SHOW USERS
```

## SHOW ROLES

[ロール](../../guides/sre/user-management/index.md#role-management)のリストを返します。他のパラメーターを表示するには、システムテーブル [system.roles](../../operations/system-tables/roles.md#system_tables-roles) および [system.role_grants](../../operations/system-tables/role-grants.md#system_tables-role_grants) を参照してください。

**構文**

``` sql
SHOW [CURRENT|ENABLED] ROLES
```

## SHOW PROFILES

[設定プロファイル](../../guides/sre/user-management/index.md#settings-profiles-management)のリストを返します。ユーザーアカウントのパラメーターを表示するには、システムテーブル [settings_profiles](../../operations/system-tables/settings_profiles.md#system_tables-settings_profiles) を参照してください。

**構文**

``` sql
SHOW [SETTINGS] PROFILES
```

## SHOW POLICIES

指定されたテーブルの[ローポリシー](../../guides/sre/user-management/index.md#row-policy-management)のリストを返します。ユーザーアカウントのパラメーターを表示するには、システムテーブル [system.row_policies](../../operations/system-tables/row_policies.md#system_tables-row_policies) を参照してください。

**構文**

``` sql
SHOW [ROW] POLICIES [ON [db.]table]
```

## SHOW QUOTAS

[クォータ](../../guides/sre/user-management/index.md#quotas-management)のリストを返します。クォータのパラメーターを表示するには、システムテーブル [system.quotas](../../operations/system-tables/quotas.md#system_tables-quotas) を参照してください。

**構文**

``` sql
SHOW QUOTAS
```

## SHOW QUOTA

全てのユーザーまたは現在のユーザーの[クォータ](../../operations/quotas.md)消費量を返します。他のパラメーターを表示するには、システムテーブル [system.quotas_usage](../../operations/system-tables/quotas_usage.md#system_tables-quotas_usage) および [system.quota_usage](../../operations/system-tables/quota_usage.md#system_tables-quota_usage) を参照してください。

**構文**

``` sql
SHOW [CURRENT] QUOTA
```

## SHOW ACCESS

すべての[ユーザー](../../guides/sre/user-management/index.md#user-account-management)、[ロール](../../guides/sre/user-management/index.md#role-management)、[プロファイル](../../guides/sre/user-management/index.md#settings-profiles-management)などとそのすべての[付与](../../sql-reference/statements/grant.md#privileges)を表示します。

**構文**

``` sql
SHOW ACCESS
```

## SHOW CLUSTER(S)

クラスタのリストを返します。すべての利用可能なクラスタは [system.clusters](../../operations/system-tables/clusters.md) テーブルにリストされています。

:::note
`SHOW CLUSTER name` クエリは、このクラスタに対する system.clusters テーブルの内容を表示します。
:::

**構文**

``` sql
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

**例**

クエリ:

``` sql
SHOW CLUSTERS;
```

結果:

```text
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

クエリ:

``` sql
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

結果:

```text
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

クエリ:

``` sql
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

結果:

```text
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
shard_weight:            1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
is_local:                1
user:                    default
default_database:
errors_count:            0
estimated_recovery_time: 0
```

## SHOW SETTINGS

システム設定とその値のリストを返します。 [system.settings](../../operations/system-tables/settings.md) テーブルからデータを選択します。

**構文**

```sql
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

**句**

`LIKE|ILIKE` は、設定名の一致パターンを指定することができます。`%` や `_` などのグロブを含むことができます。`LIKE` 句は大文字小文字を区別し、`ILIKE` は大文字小文字を区別しません。

`CHANGED` 句を使用した場合、クエリはデフォルト値から変更された設定のみを返します。

**例**

`LIKE` 句を使用したクエリ：

```sql
SHOW SETTINGS LIKE 'send_timeout';
```
結果:

```text
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

`ILIKE` 句を使用したクエリ：

```sql
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

結果:

```text
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

`CHANGED` 句を使用したクエリ：

```sql
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

結果:

```text
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

## SHOW SETTING

``` sql
SHOW SETTING <name>
```

指定された設定名の設定値を出力します。

**関連項目**
- [system.settings](../../operations/system-tables/settings.md) テーブル

## SHOW FILESYSTEM CACHES

```sql
SHOW FILESYSTEM CACHES
```

結果:

``` text
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

**関連項目**
- [system.settings](../../operations/system-tables/settings.md) テーブル

## SHOW ENGINES

``` sql
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

サーバーがサポートするテーブルエンジンとそれに関連する機能サポート情報の説明を含む [system.table_engines](../../operations/system-tables/table_engines.md) テーブルの内容を出力します。

**関連項目**
- [system.table_engines](../../operations/system-tables/table_engines.md) テーブル

## SHOW FUNCTIONS

``` sql
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

[system.functions](../../operations/system-tables/functions.md) テーブルの内容を出力します。

`LIKE` または `ILIKE` 句が指定された場合、クエリは名前が提供された `<pattern>` に一致するシステム関数のリストを返します。

**関連項目**
- [system.functions](../../operations/system-tables/functions.md) テーブル

## SHOW MERGES

マージのリストを返します。すべてのマージは [system.merges](../../operations/system-tables/merges.md) テーブルにリストされています。

- `table` -- テーブル名。
- `database` -- テーブルが存在するデータベース名。
- `estimate_complete` -- 完了までの推定時間（秒）。
- `elapsed` -- マージ開始からの経過時間（秒）。
- `progress` -- 完了した作業の割合（0-100パーセント）。
- `is_mutation` -- このプロセスが部分的な変異である場合は 1。
- `size_compressed` -- マージされたパーツの圧縮データの合計サイズ。
- `memory_usage` -- マージプロセスのメモリ消費量。

**構文**

``` sql
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

**例**

クエリ:

``` sql
SHOW MERGES;
```

結果:

```text
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

クエリ:

``` sql
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

結果:

```text
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```
