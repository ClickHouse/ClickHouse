---
slug: /ja/operations/access-rights
sidebar_position: 1
sidebar_label: ユーザーとロール
title: アクセス制御とアカウント管理
---

# ClickHouseでのユーザーとロールの作成

ClickHouseは[RBAC](https://en.wikipedia.org/wiki/Role-based_access_control)アプローチに基づくアクセス制御管理をサポートしています。

ClickHouseのアクセスエンティティ：
- [ユーザーアカウント](#user-account-management)
- [ロール](#role-management)
- [行ポリシー](#row-policy-management)
- [設定プロファイル](#settings-profiles-management)
- [クォータ](#quotas-management)

アクセスエンティティは以下の方法で設定できます：

- SQL駆動のワークフロー。

    この機能を[有効化](#enabling-access-control)する必要があります。

- サーバーの[設定ファイル](/docs/ja/operations/configuration-files.md) `users.xml` と `config.xml`。

SQL駆動のワークフローを使用することをお勧めします。両方の設定方法は同時に動作するため、アカウントやアクセス権をサーバー設定ファイルで管理している場合も、スムーズにSQL駆動のワークフローに移行できます。

:::note
同じアクセスエンティティを両方の設定方法で同時に管理することはできません。
:::

:::note
ClickHouse Cloud Consoleユーザーの管理については、この[ページ](https://clickhouse.com/docs/ja/security/cloud-access-management.md)を参照してください。
:::

すべてのユーザー、ロール、プロファイルなどとそのすべての付与を確認するには、[SHOW ACCESS](/docs/ja/sql-reference/statements/show.md#show-access-statement)ステートメントを使用します。

## 概要 {#access-control-usage}

デフォルトでは、ClickHouseサーバーにはSQL駆動のアクセス制御とアカウント管理を使用できないがすべての権限を持つ`default`ユーザーアカウントが用意されています。`default`ユーザーアカウントは、例えばクライアントからログインするときや分散クエリでユーザー名が定義されていない場合に使用されます。分散クエリ処理では、サーバーやクラスターの設定で[user and password](/docs/ja/engines/table-engines/special/distributed.md)プロパティが指定されていない場合、デフォルトのユーザーアカウントが使用されます。

ClickHouseを使い始めたばかりの場合、以下のシナリオを検討してください：

1.  `default`ユーザーに対してSQL駆動のアクセス制御とアカウント管理を[有効化](#enabling-access-control)します。
2.  `default`ユーザーアカウントにログインし、必要なすべてのユーザーを作成します。管理者アカウントを作成することを忘れずに行ってください（`GRANT ALL ON *.* TO admin_user_account WITH GRANT OPTION`）。
3.  `default`ユーザーの[権限を制限](/docs/ja/operations/settings/permissions-for-queries.md#permissions_for_queries)し、そのSQL駆動のアクセス制御とアカウント管理を無効化します。

### 現在のソリューションの特性 {#access-control-properties}

- データベースやテーブルが存在しなくても権限を付与することが可能です。
- テーブルが削除されても、そのテーブルに対応するすべての特権は取り消されません。つまり、後で同じ名前の新しいテーブルを作成しても、特権は有効なままです。削除されたテーブルに対応する特権を取り消すには、例えば`REVOKE ALL PRIVILEGES ON db.table FROM ALL`クエリを実行する必要があります。
- 特権に関する有効期限の設定は存在しません。

### ユーザーアカウント {#user-account-management}

ユーザーアカウントは、ClickHouseでの認証を可能にするアクセスエンティティです。ユーザーアカウントには以下が含まれます：

- 識別情報。
- ユーザーが実行できるクエリの範囲を定義する[特権](/docs/ja/sql-reference/statements/grant.md#privileges)。
- ClickHouseサーバーへの接続が許可されたホスト。
- 割り当てられた、またはデフォルトのロール。
- ユーザーのログイン時にデフォルトで適用される制約を持つ設定。
- 割り当てられた設定プロファイル。

特権は[GRANT](/docs/ja/sql-reference/statements/grant.md)クエリを用いてユーザーアカウントに付与されるか、[ロール](#role-management)を割り当てることによって付与されます。ユーザーから特権を取り消すには、ClickHouseは[REVOKE](/docs/ja/sql-reference/statements/revoke.md)クエリを提供します。ユーザーの特権を一覧表示するには、[SHOW GRANTS](/docs/ja/sql-reference/statements/show.md#show-grants-statement)ステートメントを使用します。

管理クエリ：

- [CREATE USER](/docs/ja/sql-reference/statements/create/user.md)
- [ALTER USER](/docs/ja/sql-reference/statements/alter/user.md#alter-user-statement)
- [DROP USER](/docs/ja/sql-reference/statements/drop.md)
- [SHOW CREATE USER](/docs/ja/sql-reference/statements/show.md#show-create-user-statement)
- [SHOW USERS](/docs/ja/sql-reference/statements/show.md#show-users-statement)

### 設定の適用 {#access-control-settings-applying}

設定は異なる方法で構成することができます：ユーザーアカウント、デフォルトで割り当てられたロールおよび設定プロファイルにおいて、ログイン時に適用される設定の値と制約は以下の通り（優先順位が高い順）です：

1.  ユーザーアカウントの設定。
2.  ユーザーアカウントのデフォルトロールの設定。ある設定が複数のロールで構成されている場合、その設定の適用順序は未定義です。
3.  ユーザーまたはそのデフォルトロールに割り当てられた設定プロファイルの設定。ある設定が複数のプロファイルで構成されている場合、その設定の適用順序は未定義です。
4.  サーバー全体にデフォルトで適用される設定または[デフォルトプロファイル](/docs/ja/operations/server-configuration-parameters/settings.md#default-profile)からの設定。

### ロール {#role-management}

ロールは、ユーザーアカウントに付与されるアクセスエンティティのコンテナです。

ロールには以下が含まれます：

- [特権](/docs/ja/sql-reference/statements/grant.md#grant-privileges)
- 設定と制約
- 割り当てられたロールのリスト

管理クエリ：

- [CREATE ROLE](/docs/ja/sql-reference/statements/create/role.md)
- [ALTER ROLE](/docs/ja/sql-reference/statements/alter/role.md#alter-role-statement)
- [DROP ROLE](/docs/ja/sql-reference/statements/drop.md)
- [SET ROLE](/docs/ja/sql-reference/statements/set-role.md)
- [SET DEFAULT ROLE](/docs/ja/sql-reference/statements/set-role.md#set-default-role-statement)
- [SHOW CREATE ROLE](/docs/ja/sql-reference/statements/show.md#show-create-role-statement)
- [SHOW ROLES](/docs/ja/sql-reference/statements/show.md#show-roles-statement)

特権は[GRANT](/docs/ja/sql-reference/statements/grant.md)クエリを使用してロールに付与できます。ロールから特権を取り消すには、ClickHouseは[REVOKE](/docs/ja/sql-reference/statements/revoke.md)クエリを提供します。

#### 行ポリシー {#row-policy-management}

行ポリシーは、ユーザーまたはロールに利用可能な行を定義するフィルターです。行ポリシーには特定のテーブル用のフィルターと、その行ポリシーを使用するロールやユーザーのリストが含まれます。

:::note
行ポリシーは読み取り専用アクセスのユーザーに対してのみ意味を持ちます。ユーザーがテーブルを変更したり、テーブル間でパーティションをコピーできる場合、行ポリシーの制限は効果が薄れます。
:::

管理クエリ：

- [CREATE ROW POLICY](/docs/ja/sql-reference/statements/create/row-policy.md)
- [ALTER ROW POLICY](/docs/ja/sql-reference/statements/alter/row-policy.md#alter-row-policy-statement)
- [DROP ROW POLICY](/docs/ja/sql-reference/statements/drop.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](/docs/ja/sql-reference/statements/show.md#show-create-row-policy-statement)
- [SHOW POLICIES](/docs/ja/sql-reference/statements/show.md#show-policies-statement)

### 設定プロファイル {#settings-profiles-management}

設定プロファイルは、[設定](/docs/ja/operations/settings/index.md)のコレクションです。設定プロファイルには、設定と制約、そしてこのプロファイルが適用されるロールやユーザーのリストが含まれます。

管理クエリ：

- [CREATE SETTINGS PROFILE](/docs/ja/sql-reference/statements/create/settings-profile.md#create-settings-profile-statement)
- [ALTER SETTINGS PROFILE](/docs/ja/sql-reference/statements/alter/settings-profile.md#alter-settings-profile-statement)
- [DROP SETTINGS PROFILE](/docs/ja/sql-reference/statements/drop.md#drop-settings-profile-statement)
- [SHOW CREATE SETTINGS PROFILE](/docs/ja/sql-reference/statements/show.md#show-create-settings-profile-statement)
- [SHOW PROFILES](/docs/ja/sql-reference/statements/show.md#show-profiles-statement)

### クォータ {#quotas-management}

クォータはリソースの使用を制限します。詳しくは[クォータ](/docs/ja/operations/quotas.md)を参照してください。

クォータは、一定期間に関する制限のセットと、このクォータを使用する必要があるロールやユーザーのリストを含みます。

管理クエリ：

- [CREATE QUOTA](/docs/ja/sql-reference/statements/create/quota.md)
- [ALTER QUOTA](/docs/ja/sql-reference/statements/alter/quota.md#alter-quota-statement)
- [DROP QUOTA](/docs/ja/sql-reference/statements/drop.md#drop-quota-statement)
- [SHOW CREATE QUOTA](/docs/ja/sql-reference/statements/show.md#show-create-quota-statement)
- [SHOW QUOTA](/docs/ja/sql-reference/statements/show.md#show-quota-statement)
- [SHOW QUOTAS](/docs/ja/sql-reference/statements/show.md#show-quotas-statement)

### SQL駆動のアクセス制御とアカウント管理の有効化 {#enabling-access-control}

- 設定の保存用ディレクトリを設定します。

    ClickHouseは、アクセスエンティティの設定を[access_control_path](/docs/ja/operations/server-configuration-parameters/settings.md#access_control_path)サーバー設定パラメータで設定されたフォルダに保存します。

- 少なくとも1つのユーザーアカウントに対してSQL駆動のアクセス制御とアカウント管理を有効化します。

    デフォルトでは、SQL駆動のアクセス制御とアカウント管理はすべてのユーザーに対して無効になっています。少なくとも1人のユーザーを`users.xml`設定ファイルに設定し、[`access_management`](/docs/ja/operations/settings/settings-users.md#access_management-user-setting)、`named_collection_control`、`show_named_collections`、`show_named_collections_secrets`設定の値を1に設定する必要があります。

## SQLユーザーとロールの定義

:::tip
ClickHouse Cloudで作業する場合は、[クラウドアクセス管理](/docs/ja/cloud/security/cloud-access-management)を参照してください。
:::

この記事では、SQLユーザーとロールの基本的な定義方法と、それらの特権と権限をデータベース、テーブル、行、カラムに適用する方法を示します。

### SQLユーザーモードの有効化

1.  `users.xml`ファイルの`<default>`ユーザーの下にSQLユーザーモードを有効にします：
    ```xml
    <access_management>1</access_management>
    <named_collection_control>1</named_collection_control>
    <show_named_collections>1</show_named_collections>
    <show_named_collections_secrets>1</show_named_collections_secrets>
    ```

    :::note
    `default`ユーザーは、新しくインストールした際に作成される唯一のユーザーであり、デフォルトではノード間の通信に使用されるアカウントでもあります。

    本番環境では、ノード間通信がSQL管理者ユーザーで設定されている場合や、`<secret>`、クラスターの認証情報、および/またはノード間HTTPおよびトランスポートプロトコルの認証情報で設定されている場合には、このユーザーを無効にすることを推奨します。
    :::

2. ノードを再起動して、変更を適用します。

3. ClickHouseクライアントを起動します：
    ```sql
    clickhouse-client --user default --password <password>
    ```
### ユーザーの定義

1. SQL管理者アカウントを作成します：
    ```sql
    CREATE USER clickhouse_admin IDENTIFIED BY 'password';
    ```
2. 新しいユーザーに完全な管理権限を付与します：
    ```sql
    GRANT ALL ON *.* TO clickhouse_admin WITH GRANT OPTION;
    ```

<Content />

## ALTER権限

この記事は、特権をどのように定義し、特権ユーザーに対して`ALTER`ステートメントを使用する場合の権限がどのように機能するかをよりよく理解するためのものです。

`ALTER`ステートメントは、いくつかのカテゴリーに分かれています。その中には階層的なものと、明示的に定義されるべきものが存在します。

**DB、テーブル、ユーザーの構成例**

1. 管理者ユーザーでサンプルユーザーを作成します：
```sql
CREATE USER my_user IDENTIFIED BY 'password';
```

2. サンプルデータベースを作成します：
```sql
CREATE DATABASE my_db;
```

3. サンプルテーブルを作成します：
```sql
CREATE TABLE my_db.my_table (id UInt64, column1 String) ENGINE = MergeTree() ORDER BY id;
```

4. 権限の付与/取消しを行うサンプル管理者ユーザーを作成します：
```sql
CREATE USER my_alter_admin IDENTIFIED BY 'password';
```

:::note
権限を付与または取消するには、管理者ユーザーが`WITH GRANT OPTION`特権を持っている必要があります。
例えば：
  ```sql
  GRANT ALTER ON my_db.* WITH GRANT OPTION
  ```
ユーザーに権限を付与または取り消すためには、まずそのユーザー自身がこれらの権限を持っている必要があります。
:::

**権限の付与または取消し**

`ALTER`の階層：

```
.
├── ALTER (テーブルとビューのみ)/
│   ├── ALTER TABLE/
│   │   ├── ALTER UPDATE
│   │   ├── ALTER DELETE
│   │   ├── ALTER COLUMN/
│   │   │   ├── ALTER ADD COLUMN
│   │   │   ├── ALTER DROP COLUMN
│   │   │   ├── ALTER MODIFY COLUMN
│   │   │   ├── ALTER COMMENT COLUMN
│   │   │   ├── ALTER CLEAR COLUMN
│   │   │   └── ALTER RENAME COLUMN
│   │   ├── ALTER INDEX/
│   │   │   ├── ALTER ORDER BY
│   │   │   ├── ALTER SAMPLE BY
│   │   │   ├── ALTER ADD INDEX
│   │   │   ├── ALTER DROP INDEX
│   │   │   ├── ALTER MATERIALIZE INDEX
│   │   │   └── ALTER CLEAR INDEX
│   │   ├── ALTER CONSTRAINT/
│   │   │   ├── ALTER ADD CONSTRAINT
│   │   │   └── ALTER DROP CONSTRAINT
│   │   ├── ALTER TTL/
│   │   │   └── ALTER MATERIALIZE TTL
│   │   ├── ALTER SETTINGS
│   │   ├── ALTER MOVE PARTITION
│   │   ├── ALTER FETCH PARTITION
│   │   └── ALTER FREEZE PARTITION
│   └── ALTER LIVE VIEW/
│       ├── ALTER LIVE VIEW REFRESH
│       └── ALTER LIVE VIEW MODIFY QUERY
├── ALTER DATABASE
├── ALTER USER
├── ALTER ROLE
├── ALTER QUOTA
├── ALTER [ROW] POLICY
└── ALTER [SETTINGS] PROFILE
```

1. ユーザーまたはロールに`ALTER`権限を付与する

`GRANT ALTER on *.* TO my_user`を使用すると、トップレベルの`ALTER TABLE`と`ALTER VIEW`にのみ影響し、その他の`ALTER`ステートメントは個別に付与または取り消す必要があります。

例えば、基本的な`ALTER`特権を付与する：
```sql
GRANT ALTER ON my_db.my_table TO my_user;
```

結果として付与される特権のセット：
```sql
SHOW GRANTS FOR  my_user;
```

```response
SHOW GRANTS FOR my_user

Query id: 706befbc-525e-4ec1-a1a2-ba2508cc09e3

┌─GRANTS FOR my_user───────────────────────────────────────────┐
│ GRANT ALTER TABLE, ALTER VIEW ON my_db.my_table TO my_user   │
└──────────────────────────────────────────────────────────────┘
```

これにより、上記の例から`ALTER TABLE`と`ALTER VIEW`の下にあるすべての権限が付与されますが、`ALTER ROW POLICY`などの他の`ALTER`権限は付与されません（階層図に戻ると、`ALTER ROW POLICY`が`ALTER TABLE`または`ALTER VIEW`の子でないことがわかります）。これらは明示的に付与または取り消す必要があります。

例えば、次のように一部の`ALTER`特権が必要な場合、それぞれを別々に付与できます。副権限がある場合、それらも自動的に付与されます。

例：
```sql
GRANT ALTER COLUMN ON my_db.my_table TO my_user;
```

付与されたものは次のようになります：
```sql
SHOW GRANTS FOR my_user;
```

```response
SHOW GRANTS FOR my_user

Query id: 47b3d03f-46ac-4385-91ec-41119010e4e2

┌─GRANTS FOR my_user────────────────────────────────┐
│ GRANT ALTER COLUMN ON default.my_table TO my_user │
└───────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.004 sec.
```

これにより、次のような副権限も与えられます：
```sql
ALTER ADD COLUMN
ALTER DROP COLUMN
ALTER MODIFY COLUMN
ALTER COMMENT COLUMN
ALTER CLEAR COLUMN
ALTER RENAME COLUMN
```

2. ユーザーおよびロールから`ALTER`権限を取り消す

`REVOKE`ステートメントは、`GRANT`ステートメントと同様に機能します。

ユーザーまたはロールに副権限が付与された場合、その副権限を直接取り消すか、次の上位レベルの権限を取り消すことができます。

例：ユーザーに`ALTER ADD COLUMN`が付与された場合
```sql
GRANT ALTER ADD COLUMN ON my_db.my_table TO my_user;
```

```response
GRANT ALTER ADD COLUMN ON my_db.my_table TO my_user

Query id: 61fe0fdc-1442-4cd6-b2f3-e8f2a853c739

Ok.

0 rows in set. Elapsed: 0.002 sec.
```

```sql
SHOW GRANTS FOR my_user;
```

```response
SHOW GRANTS FOR my_user

Query id: 27791226-a18f-46c8-b2b4-a9e64baeb683

┌─GRANTS FOR my_user──────────────────────────────────┐
│ GRANT ALTER ADD COLUMN ON my_db.my_table TO my_user │
└─────────────────────────────────────────────────────┘
```

権限を個別に取り消すこともできます：
```sql
REVOKE ALTER ADD COLUMN ON my_db.my_table FROM my_user;
```

あるいは、どの上位レベルからでも取り消すことができます（すべてのCOLUMN副権限を取り消します）：
```
REVOKE ALTER COLUMN ON my_db.my_table FROM my_user;
```

```response
REVOKE ALTER COLUMN ON my_db.my_table FROM my_user

Query id: b882ba1b-90fb-45b9-b10f-3cda251e2ccc

Ok.

0 rows in set. Elapsed: 0.002 sec.
```

```sql
SHOW GRANTS FOR my_user;
```

```response
SHOW GRANTS FOR my_user

Query id: e7d341de-de65-490b-852c-fa8bb8991174

Ok.

0 rows in set. Elapsed: 0.003 sec.
```

**補足**

権限は、`WITH GRANT OPTION`を持つユーザーによって付与される必要があり、さらにそのユーザー自身がその権限を持っている必要があります。

1. 管理者ユーザーに権限を付与し、特権のセットを管理することを許可する場合
例：
```sql
GRANT SELECT, ALTER COLUMN ON my_db.my_table TO my_alter_admin WITH GRANT OPTION;
```

このユーザーは、`ALTER COLUMN`およびすべてのサブ特権を付与または取り消すことができます。

**テスト**

1. `SELECT`権限を追加します：
```sql
 GRANT SELECT ON my_db.my_table TO my_user;
```

2. ユーザーにカラム追加権限を与える：
```sql
GRANT ADD COLUMN ON my_db.my_table TO my_user;
```

3. 制限付きユーザーでログインする：
```bash
clickhouse-client --user my_user --password password --port 9000 --host <your_clickhouse_host>
```

4. カラムを追加してみる：
```sql
ALTER TABLE my_db.my_table ADD COLUMN column2 String;
```

```response
ALTER TABLE my_db.my_table
    ADD COLUMN `column2` String

Query id: d5d6bfa1-b80c-4d9f-8dcd-d13e7bd401a5

Ok.

0 rows in set. Elapsed: 0.010 sec.
```

```sql
DESCRIBE my_db.my_table;
```

```response
DESCRIBE TABLE my_db.my_table

Query id: ab9cb2d0-5b1a-42e1-bc9c-c7ff351cb272

┌─name────┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ UInt64 │              │                    │         │                  │                │
│ column1 │ String │              │                    │         │                  │                │
│ column2 │ String │              │                    │         │                  │                │
└─────────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

4. カラムを削除してみる：
```sql
ALTER TABLE my_db.my_table DROP COLUMN column2;
```

```response
ALTER TABLE my_db.my_table
    DROP COLUMN column2

Query id: 50ad5f6b-f64b-4c96-8f5f-ace87cea6c47


0 rows in set. Elapsed: 0.004 sec.

Received exception from server (version 22.5.1):
Code: 497. DB::Exception: Received from chnode1.marsnet.local:9440. DB::Exception: my_user: Not enough privileges. To execute this query it's necessary to have grant ALTER DROP COLUMN(column2) ON my_db.my_table. (ACCESS_DENIED)
```

5. alter管理者を利用して権限を付与するテスト
```sql
GRANT SELECT, ALTER COLUMN ON my_db.my_table TO my_alter_admin WITH GRANT OPTION;
```

6. alter管理者ユーザーでログイン：
```bash
clickhouse-client --user my_alter_admin --password password --port 9000 --host <my_clickhouse_host>
```

7. サブ特権を付与する：
```sql
GRANT ALTER ADD COLUMN ON my_db.my_table TO my_user;
```

```response
GRANT ALTER ADD COLUMN ON my_db.my_table TO my_user

Query id: 1c7622fa-9df1-4c54-9fc3-f984c716aeba

Ok.
```

8. 権限を持たないalter管理者ユーザーが持っていないサブ特権を持たない特権を付与しようとするテスト。
```sql
GRANT ALTER UPDATE ON my_db.my_table TO my_user;
```

```response
GRANT ALTER UPDATE ON my_db.my_table TO my_user

Query id: 191690dc-55a6-4625-8fee-abc3d14a5545


0 rows in set. Elapsed: 0.004 sec.

Received exception from server (version 22.5.1):
Code: 497. DB::Exception: Received from chnode1.marsnet.local:9440. DB::Exception: my_alter_admin: Not enough privileges. To execute this query it's necessary to have grant ALTER UPDATE ON my_db.my_table WITH GRANT OPTION. (ACCESS_DENIED)
```

**まとめ**

`ALTER`特権はテーブルおよびビューに対する`ALTER`に対して階層的ですが、他の`ALTER`ステートメントには適用されません。特権は細かいレベルや特権のグループに設定でき、同様に取り消すこともできます。権限を設定するユーザーは、`WITH GRANT OPTION`を持っている必要があり、設定するユーザー自身を含む、その権限を既に持っている必要があります。操作を行うユーザーは、もし付与オプション権限を自分でも持っていない場合、自分自身の特権を取り消すことはできません。

