---
description: 'テーブル関数を使用すると、YTsaurus クラスターからデータを読み取れます。'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # ytsaurus テーブル関数
</div>

<ExperimentalBadge />

このテーブル関数を使用すると、YTsaurus クラスターのデータを読み取れます。

<div id="syntax">
  ## 構文
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
これは Experimental な機能であり、将来のリリースで後方互換性のない変更が加わる可能性があります。
YTsaurus テーブル関数 を有効にするには、
[allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/ja/operations/settings/settings#allow_experimental_ytsaurus_table_engine) 設定を使用します。
`set allow_experimental_ytsaurus_table_function = 1` コマンドを入力します。
:::

<div id="arguments">
  ## 引数
</div>

* `http_proxy_url` — YTsaurus HTTPプロキシの URL。
* `cypress_path` — データソースへの Cypress パス。
* `oauth_token` — OAuth トークン。
* `format` — データソースの [フォーマット](/ja/interfaces/formats)。

**戻り値**

YTsaurus クラスター内の指定した YTsaurus Cypress パスにあるデータを読み取るための、指定した構造を持つテーブル。

**関連項目**

* [YTsaurus エンジン](/ja/engines/table-engines/integrations/ytsaurus.md)