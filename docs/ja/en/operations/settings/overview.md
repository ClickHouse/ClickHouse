---
description: '設定の概要ページ。'
sidebar_position: 1
slug: /operations/settings/overview
title: '設定の概要'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

:::note
XML ベースの設定プロファイルと[設定ファイル](/ja/operations/configuration-files)は現在、
ClickHouse Cloud ではサポートされていません。ClickHouse Cloud
サービスの設定を指定するには、[SQL ベースの設定プロファイル](/ja/operations/access-rights#settings-profiles-management)を使用する必要があります。
:::

ClickHouse の設定は、主に次のグループに分けられます。

* グローバルなサーバー設定
* セッション設定
* クエリ設定
* バックグラウンド処理の設定

グローバル設定は、下位レベルで上書きされない限り、デフォルトで適用されます。セッション設定は、プロファイル、ユーザー設定、および SET コマンドで指定できます。クエリ設定は SETTINGS 句で指定でき、個々のクエリに適用されます。バックグラウンド処理の設定は、Mutations、Merges、および場合によってはその他の操作に適用され、これらはバックグラウンドで非同期に実行されます。

<div id="see-non-default-settings">
  ## デフォルト以外の設定を表示する
</div>

どの設定がデフォルト値から変更されているかを確認するには、
`system.settings` テーブルをクエリします:

```sql
SELECT name, value FROM system.settings WHERE changed
```

設定がデフォルト値から一切変更されていない場合、ClickHouse は
何も返しません。

特定の設定の値を確認するには、クエリでその設定の `name` を
指定します。

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

次のような結果が返されます。

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## 参考資料
</div>

* [グローバルサーバー設定](/ja/operations/server-configuration-parameters/settings.md) を参照して、グローバルサーバーレベルで
  ClickHouse server を設定する方法の詳細を確認してください。
* [セッション設定](/ja/operations/settings/settings-query-level.md) を参照して、セッションレベルで ClickHouse
  server を設定する方法の詳細を確認してください。
* [Context の階層構造](/ja/development/architecture.md#context) を参照して、ClickHouse における設定処理の詳細を確認してください。