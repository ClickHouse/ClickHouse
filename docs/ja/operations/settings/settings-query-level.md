---
sidebar_label: クエリレベルの設定
title: クエリレベルの設定
slug: /ja/operations/settings/query-level
---

ClickHouseのクエリレベル設定を設定する方法はいくつかあります。設定はレイヤーとして構成され、後のレイヤーは前の設定値を再定義します。

設定を定義する際の優先順位は以下の通りです：

1. ユーザーに直接、または設定プロファイル内で設定を適用する

    - SQL（推奨）
    - 一つまたは複数のXMLまたはYAMLファイルを`/etc/clickhouse-server/users.d`に追加する 

2. セッション設定

    - ClickHouse Cloud SQLコンソールまたはインタラクティブモードの`clickhouse client`から`SET setting=value`を送信する。同様に、HTTPプロトコルでClickHouseセッションを使用することもできます。これを行うには、`session_id` HTTPパラメータを指定する必要があります。

3. クエリ設定

    - 非インタラクティブモードで`clickhouse client`を起動する際に、スタートパラメータ`--setting=value`を設定する。
    - HTTP APIを使用する場合、CGIパラメータを渡す（`URL?setting_1=value&setting_2=value...`）。
    - SELECTクエリの[SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)句で設定を定義する。設定値はそのクエリに対してのみ適用され、クエリ実行後にデフォルトまたは前の値にリセットされます。

## 例

これらの例ではすべて、`async_insert`設定の値を`1`に設定し、実行中のシステムでの設定の確認方法を示します。

### SQLを使用してユーザーに直接設定を適用する

これは、設定`async_inset = 1`を持つユーザー`ingester`を作成します：

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
# highlight-next-line
SETTINGS async_insert = 1
```

#### 設定プロファイルと割り当てを確認

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
### SQLを使用して設定プロファイルを作成しユーザーに割り当てる

これは、設定`async_inset = 1`を持つプロファイル`log_ingest`を作成します：

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

これは、ユーザー`ingester`を作成し、ユーザーに設定プロファイル`log_ingest`を割り当てます：

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
# highlight-next-line
SETTINGS PROFILE log_ingest
```


### XMLを使用して設定プロファイルとユーザーを作成する

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

#### 設定プロファイルと割り当てを確認

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

### セッションに設定を割り当てる

```sql
SET async_insert = 1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

### クエリ中に設定を割り当てる

```sql
INSERT INTO YourTable
# highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

## 設定をデフォルト値に戻す

設定を変更し、それをデフォルト値に戻したい場合、値を`DEFAULT`に設定します。その構文は以下の通りです：

```sql
SET setting_name = DEFAULT
```

例えば、`async_insert`のデフォルト値は`0`です。値を`1`に変更したとします：

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

レスポンスは以下の通りです：

```response
┌─value──┐
│ 1      │
└────────┘
```

次のコマンドはその値を0に戻します：

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

設定はデフォルトに戻りました：

```response
┌─value───┐
│ 0       │
└─────────┘
```

## カスタム設定 {#custom_settings}

共通の[設定](../../operations/settings/settings.md)に加えて、ユーザーはカスタム設定を定義することができます。

カスタム設定名は、指定されたプレフィックスのいずれかで始める必要があります。これらのプレフィックスのリストは、サーバー設定ファイルの[custom_settings_prefixes](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes)パラメータで宣言する必要があります。

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

カスタム設定を定義するには`SET`コマンドを使用します：

```sql
SET custom_a = 123;
```

カスタム設定の現在の値を取得するには、`getSetting()`関数を使用します：

```sql
SELECT getSetting('custom_a');
```

**関連情報**

- ClickHouse設定の説明については、[設定](./settings.md)ページを参照してください。
- [グローバルサーバー設定](../../operations/server-configuration-parameters/settings.md)
