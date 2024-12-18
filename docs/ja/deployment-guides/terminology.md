---
slug: /ja/architecture/introduction
sidebar_label: はじめに
title: はじめに
sidebar_position: 1
---
import ReplicationShardingTerminology from '@site/docs/ja/_snippets/_replication-sharding-terminology.md';

これらのデプロイメント例は、ClickHouse Support and Services 組織が ClickHouse ユーザーに提供したアドバイスに基づいています。これらは実際に動作する例であり、ぜひ試してみてからニーズに合わせて調整することをお勧めします。ここで、あなたの要件にぴったり合う例を見つけるかもしれません。あるいは、データが2回ではなく3回レプリケートされる必要がある場合、ここで示されているパターンに従ってもう1つのレプリカを追加することができるでしょう。

<ReplicationShardingTerminology />

## 例

### 基本

- [**スケールアウト**](/docs/ja/deployment-guides/horizontal-scaling.md)の例は、データを2つのノードにシャードし、分散テーブルを使用する方法を示しています。これにより、データが2つの ClickHouse ノードに存在することになります。2つの ClickHouse ノードは、分散同期を提供する ClickHouse Keeper も実行します。3つ目のノードは、ClickHouse Keeper クォーラムを完了するために単独で ClickHouse Keeper を実行します。

- [**フォルトトレランスのためのレプリケーション**](/docs/ja/deployment-guides/replicated.md)の例は、データを2つのノードにレプリケートし、ReplicatedMergeTree テーブルを使用する方法を示しています。これにより、データが2つの ClickHouse ノードに存在することになります。2つの ClickHouse サーバーノードに加えて、レプリケーションを管理するための3つの ClickHouse Keeper 単独ノードがあります。

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/vBjCJtw_Ei0"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

### 中級

- 近日公開

### 上級

- 近日公開
