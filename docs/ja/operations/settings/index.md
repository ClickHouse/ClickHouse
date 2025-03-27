---
title: "設定の概要"
sidebar_position: 1
slug: /ja/operations/settings/
---

# 設定の概要

:::note
XMLベースの設定プロファイルと[設定ファイル](https://clickhouse.com/docs/ja/operations/configuration-files)は、現在のところClickHouse Cloudではサポートされていません。ClickHouse Cloudサービスの設定を指定するには、[SQL駆動の設定プロファイル](https://clickhouse.com/docs/ja/operations/access-rights#settings-profiles-management)を使用する必要があります。
:::

ClickHouseの設定は主に2つのグループに分かれています:

- グローバルサーバー設定
- クエリレベルの設定

グローバルサーバー設定とクエリレベルの設定の主な違いは、グローバルサーバー設定は設定ファイルで設定する必要があり、クエリレベルの設定は設定ファイルまたはSQLクエリで設定できるという点です。

ClickHouseサーバーをグローバルサーバーレベルで設定する方法については、[グローバルサーバー設定](/docs/ja/operations/server-configuration-parameters/settings.md)を参照してください。

ClickHouseサーバーをクエリレベルで設定する方法については、[クエリレベルの設定](/docs/ja/operations/settings/settings-query-level.md)を参照してください。

## デフォルト以外の設定を確認する

デフォルトの値から変更された設定を確認するには、次のクエリを使用します:

```sql
SELECT name, value FROM system.settings WHERE changed
```

デフォルトの値から設定を変更していない場合、ClickHouseは何も返しません。

特定の設定の値を確認するには、クエリ内で設定の`name`を指定します:

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

このコマンドは以下のような結果を返します:

```response
┌─name────────┬─value─────┐
│ max_threads │ 'auto(8)' │
└─────────────┴───────────┘

1 row in set. Elapsed: 0.002 sec.
```
