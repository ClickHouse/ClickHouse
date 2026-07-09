---
description: 'クエリレベルのセッション設定'
sidebar_label: 'クエリレベルのセッション設定'
slug: /operations/settings/query-level
title: 'クエリレベルのセッション設定'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

特定の設定を指定してステートメントを実行する方法は複数あります。
設定はレイヤーごとに構成され、後続の各レイヤーでそれまでの設定値が上書きされます。

<div id="order-of-priority">
  ## 優先順位
</div>

設定を定義する場合の優先順位は次のとおりです。

1. 設定をユーザーに直接適用する、または設定プロファイルで適用する

   * SQL (推奨)
   * 1 つ以上の XML または YAML ファイルを `/etc/clickhouse-server/users.d` に追加する

2. セッション設定

   * ClickHouse Cloud の SQL コンソール、または対話型モードの
     `clickhouse client` から `SET setting=value` を送信します。同様に、HTTP プロトコルで ClickHouse
     セッションを使用することもできます。これを行うには、
     HTTP パラメータ `session_id` を指定する必要があります。

3. クエリ設定

   * `clickhouse client` を非対話型モードで起動する際に、起動
     パラメータ `--setting=value` を指定します。
   * HTTP API を使用する場合は、CGI パラメータ (`URL?setting_1=value&setting_2=value...`) を指定します。
   * SELECT クエリの
     [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     句で設定を定義します。設定値はそのクエリにのみ適用され、
     クエリの実行後にデフォルト値または以前の値にリセットされます。

<div id="converting-a-setting-to-its-default-value">
  ## 設定をデフォルト値に戻す
</div>

設定を変更した後でデフォルト値に戻したい場合は、値を `DEFAULT` に設定します。構文は次のとおりです。

```sql
SET setting_name = DEFAULT
```

たとえば、`async_insert` のデフォルト値は `0` です。これを `1` に変更する場合:

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

レスポンスは次のとおりです:

```response
┌─value──┐
│ 1      │
└────────┘
```

次のコマンドで、その値を 0 に戻せます。

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

この設定はデフォルトに戻りました。

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## カスタム設定
</div>

共通の[設定](/ja/operations/settings/settings.md)に加えて、ユーザーはカスタム設定を定義できます。
カスタム設定を使うと、クエリ、ポリシー、関数内で参照できる**セッション固有のパラメーター**を渡せます。これは、次のような場合に便利です。

* ユーザーの識別情報や組織に基づいてデータをフィルタリングする
* コンテキストに応じて異なるビジネスロジックを適用する
* セッション内のクエリ間でステートフルな情報を維持する

カスタム設定名は、定義したリストに含まれる、あらかじめ定義されたプレフィックスのいずれかで始まる必要があります。
プレフィックスのリストは、サーバー設定ファイルで定義する[`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes)サーバー設定を使用して指定できます。

以下の例では、`SQL_`をカスタムプレフィックスとして選択しています。

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
ClickHouse Cloud では、カスタムプレフィックスは指定できません。
すべてのカスタムユーザー設定は、`SQL_` プレフィックスで始まります。
:::

カスタム設定を定義するには、`SET` コマンドを使用します。

```sql
SET SQL_a = 123;
```

カスタム設定の現在の値を取得するには、`getSetting()` 関数を使用します:

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## 例
</div>

これらの例では、いずれも `async_insert` 設定の値を `1` に設定し、
稼働中のシステムで設定を確認する方法を示します。

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### SQLでユーザーに設定を直接適用する
</div>

これにより、設定 `async_inset = 1` を持つユーザー `ingester` が作成されます。

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### 設定プロファイルと割り当ての確認
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### SQL を使用して設定プロファイルを作成し、ユーザーに割り当てる
</div>

これにより、設定 `async_inset = 1` を持つプロファイル `log_ingest` が作成されます:

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

これにより、ユーザー `ingester` が作成され、そのユーザーに設定プロファイル `log_ingest` が割り当てられます。

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### XMLで設定プロファイルとユーザーを作成する
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment-1">
  #### 設定プロファイルとその割り当てを確認する
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### セッションに設定を適用する
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### クエリ実行時に設定を指定する
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## 関連項目
</div>

* ClickHouse settings の説明については、[Settings](/ja/operations/settings/settings.md) ページを参照してください。
* [グローバルサーバー設定](/ja/operations/server-configuration-parameters/settings.md)