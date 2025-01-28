---
sidebar_label: "一般的なアクセス管理クエリ"
title: "一般的なアクセス管理クエリ"
slug: "/ja/cloud/security/common-access-management-queries"
---

import CommonUserRolesContent from '@site/docs/ja/_snippets/_users-and-roles-common.md';

# 一般的なアクセス管理クエリ

:::tip セルフマネージド
セルフマネージドのClickHouseを使用している場合は、[SQLユーザーとロール](/docs/ja/guides/sre/user-management/index.md)をご覧ください。
:::

この記事では、SQLユーザーとロールを定義し、それらの特権と権限をデータベース、テーブル、行、カラムに適用する基本的な方法を示します。

## 管理ユーザー

ClickHouse Cloudサービスには、サービス作成時に作成される`default`という管理ユーザーがいます。パスワードはサービス作成時に提供され、**Admin**のロールを持つClickHouse Cloudユーザーによってリセットできます。

ClickHouse Cloudサービスに追加のSQLユーザーを追加する場合、そのユーザーにはSQLユーザー名とパスワードが必要です。管理レベルの特権を付与したい場合は、新しいユーザーに`default_role`ロールを割り当ててください。例えば、ユーザー`clickhouse_admin`を追加します:

```sql
CREATE USER IF NOT EXISTS clickhouse_admin
IDENTIFIED WITH sha256_password BY 'P!@ssword42!';
```

```sql
GRANT default_role TO clickhouse_admin;
```

:::note
SQLコンソールを使用する場合、SQLステートメントは`default`ユーザーとして実行されません。代わりに、`sql-console:${cloud_login_email}`という名前のユーザーとして実行され、`cloud_login_email`はクエリを現在実行しているユーザーのメールアドレスです。

これらの自動生成されたSQLコンソールユーザーには`default`ロールが付与されています。
:::

## パスワードなし認証

SQLコンソールには2つのロールが利用可能です: `sql_console_admin`は`default_role`と同じ権限を持ち、`sql_console_read_only`は読み取り専用の権限を持ちます。

管理ユーザーにはデフォルトで`sql_console_admin`ロールが割り当てられているため、何も変更はありません。ただし、`sql_console_read_only`ロールを使用すると、管理者でないユーザーでも読み取り専用または完全なアクセス権を任意のインスタンスに割り当てることができます。管理者がこのアクセスを設定する必要があります。これらのロールは`GRANT`または`REVOKE`コマンドを使用してインスタンス固有の要件に合わせて調整できます。これらのロールに対する変更は永続化されます。

### 詳細なアクセス制御

このアクセス制御機能は、ユーザーレベルの詳細度で手動で設定することもできます。新しい`sql_console_*`ロールをユーザーに割り当てる前に、`sql-console-role:<email>`という名前空間に一致するSQLコンソールユーザー専用のデータベースロールを作成する必要があります。例えば:

```sql
CREATE ROLE OR REPLACE sql-console-role:<email>;
GRANT <some grants> TO sql-console-role:<email>;
```

一致するロールが検出されると、テンプレートロールではなく、そのユーザーに割り当てられます。より複雑なアクセス制御の設定が可能となり、例えば`sql_console_sa_role`や`sql_console_pm_role`のようなロールを作成し、特定のユーザーに付与することができます。例えば:

```sql
CREATE ROLE OR REPLACE sql_console_sa_role;
GRANT <whatever level of access> TO sql_console_sa_role;
CREATE ROLE OR REPLACE sql_console_pm_role;
GRANT <whatever level of access> TO sql_console_pm_role;
CREATE ROLE OR REPLACE `sql-console-role:christoph@clickhouse.com`;
CREATE ROLE OR REPLACE `sql-console-role:jake@clickhouse.com`;
CREATE ROLE OR REPLACE `sql-console-role:zach@clickhouse.com`;
GRANT sql_console_sa_role to `sql-console-role:christoph@clickhouse.com`;
GRANT sql_console_sa_role to `sql-console-role:jake@clickhouse.com`;
GRANT sql_console_pm_role to `sql-console-role:zach@clickhouse.com`;
```

<CommonUserRolesContent />
