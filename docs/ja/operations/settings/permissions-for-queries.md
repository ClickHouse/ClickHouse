---
slug: /ja/operations/settings/permissions-for-queries
sidebar_position: 58
sidebar_label: クエリの権限
---

# クエリの権限

ClickHouseのクエリは以下のいくつかのタイプに分けられます。

1. データ読み取りクエリ: `SELECT`、`SHOW`、`DESCRIBE`、`EXISTS`
2. データ書き込みクエリ: `INSERT`、`OPTIMIZE`
3. 設定変更クエリ: `SET`、`USE`
4. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) クエリ: `CREATE`、`ALTER`、`RENAME`、`ATTACH`、`DETACH`、`DROP`、`TRUNCATE`
5. `KILL QUERY`

以下の設定は、クエリのタイプによってユーザー権限を調整します。

## readonly
データの読み取り、データの書き込み、および設定変更クエリに対する権限を制限します。

1に設定すると、以下が許可されます:

- 全てのタイプの読み取りクエリ（`SELECT`や同等のクエリなど）
- セッションコンテキストのみを変更するクエリ（`USE`など）

2に設定すると、上記に加えて以下も許可されます:
- `SET` と `CREATE TEMPORARY TABLE`

  :::tip
  `EXISTS`、`DESCRIBE`、`EXPLAIN`、`SHOW PROCESSLIST` などのクエリは、システムテーブルからの選択を行うだけなので、`SELECT` と同等です。
  :::

可能な値:
- 0 — 読み取り、書き込み、設定変更クエリが許可されます。
- 1 — データ読み取りクエリのみが許可されます。
- 2 — データ読み取りおよび設定変更クエリが許可されます。

デフォルト値: 0

:::note
`readonly = 1` を設定した後、現在のセッションで `readonly` および `allow_ddl` の設定を変更することはできません。

[HTTPインターフェース](../../interfaces/http.md)で`GET`メソッドを使用する場合、`readonly = 1` が自動的に設定されます。データを変更するには、`POST`メソッドを使用してください。

`readonly = 1` を設定すると、ユーザーは設定を変更できなくなります。特定の設定のみの変更を禁止する方法もあります。また、`readonly = 1` の制限下で特定の設定のみの変更を許可する方法もあります。詳細は[設定の制約](../../operations/settings/constraints-on-settings.md)を参照してください。
:::

## allow_ddl {#allow_ddl}

[DDL](https://en.wikipedia.org/wiki/Data_definition_language)クエリを許可または禁止します。

可能な値:

- 0 — DDLクエリは許可されません。
- 1 — DDLクエリは許可されます。

デフォルト値: 1

:::note
現在のセッションで `allow_ddl = 0` の場合、`SET allow_ddl = 1` を実行することはできません。
:::

:::note KILL QUERY
`KILL QUERY` は、`readonly` と `allow_ddl` の設定のいかなる組み合わせでも実行可能です。
:::
