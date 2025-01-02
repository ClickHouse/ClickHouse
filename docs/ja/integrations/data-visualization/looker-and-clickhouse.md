---
sidebar_label: Looker
slug: /ja/integrations/looker
keywords: [clickhouse, looker, connect, integrate, ui]
description: Lookerは、リアルタイムでインサイトを探索し共有するのに役立つエンタープライズプラットフォームであり、BI、データアプリケーション、および組み込み分析を提供します。
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Looker

Lookerは、公式のClickHouseデータソースを通じて、ClickHouse Cloudまたはオンプレミスのデプロイメントに接続できます。

## 1. 接続詳細の収集
<ConnectionDetails />

## 2. ClickHouseデータソースを作成する

管理者 -> データベース -> 接続に移動し、右上の「接続を追加」ボタンをクリックします。

<img src={require('./images/looker_01.png').default} class="image" alt="新しい接続を追加" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

データソースの名前を選択し、方言のドロップダウンから `ClickHouse` を選びます。フォームに資格情報を入力します。

<img src={require('./images/looker_02.png').default} class="image" alt="資格情報を指定" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

ClickHouse Cloudを使用している場合やデプロイメントにSSLが必要な場合は、追加の設定でSSLをオンにしていることを確認してください。

<img src={require('./images/looker_03.png').default} class="image" alt="SSLを有効化" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

まず接続をテストし、それが完了したら、新しいClickHouseデータソースに接続します。

<img src={require('./images/looker_04.png').default} class="image" alt="接続テスト完了" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

これで、LookerプロジェクトにClickHouseデータソースをアタッチできるようになります。

## 3. 既知の制限事項

1. 以下のデータ型はデフォルトで文字列として処理されます:
   * Array - JDBCドライバーの制限によりシリアル化が予期した通りに動作しません
   * Decimal* - モデル内で数値に変更できます
   * LowCardinality(...) - モデル内で適切な型に変更できます
   * Enum8, Enum16
   * UUID
   * Tuple
   * Map
   * JSON
   * Nested
   * FixedString
   * Geo types
     * MultiPolygon
     * Polygon
     * Point
     * Ring
2. [対称集計機能](https://cloud.google.com/looker/docs/reference/param-explore-symmetric-aggregates)はサポートされていません
3. [フル外部結合](https://cloud.google.com/looker/docs/reference/param-explore-join-type#full_outer)はドライバーでまだ実装されていません
