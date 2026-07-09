---
description: 'ClickHouse Cloud で利用可能な `Shared` データベースエンジンについて説明するページ'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # 共有データベースエンジン
</div>

`Shared` データベースエンジンは、[`SharedMergeTree`](/ja/cloud/reference/shared-merge-tree) などのステートレスなテーブルエンジンを使用するテーブルを持つデータベースを管理するために、Shared Catalog と連携して動作します。
これらのテーブルエンジンは永続的な状態をディスクに書き込まず、動的なコンピュート環境に対応しています。

Cloud の `Shared` データベースエンジンでは、ローカルディスクへの依存がなくなります。
これは完全にインメモリのエンジンで、必要なのは CPU とメモリだけです。

<div id="how-it-works">
  ## どのような仕組みですか？
</div>

`Shared` データベースエンジンは、Keeper を基盤とする中央の Shared Catalog に、すべてのデータベースおよびテーブル定義を保存します。ローカルディスクに書き込む代わりに、すべてのコンピュートノードで共有される単一のバージョン付きグローバル状態を維持します。

各ノードは最後に適用したバージョンだけを追跡し、起動時にはローカルファイルや手動でのセットアップなしで最新の状態を取得します。

<div id="syntax">
  ## 構文
</div>

エンドユーザーが Shared Catalog と共有データベースエンジンを使用するために、追加の設定は必要ありません。データベースの作成方法は従来と同じです。

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud では、データベースに 共有データベースエンジン が自動的に割り当てられます。このようなデータベース内でステートレスエンジンを使って作成されたテーブルは、Shared Catalog のレプリケーション機能と協調機能の恩恵を自動的に受けられます。

:::tip
Shared Catalog とその利点の詳細については、Cloud リファレンスの [「Shared Catalog と共有データベースエンジン」](/ja/cloud/reference/shared-catalog) を参照してください。
:::