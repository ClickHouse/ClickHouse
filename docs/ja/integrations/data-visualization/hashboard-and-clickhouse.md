---
sidebar_label: Hashboard
sidebar_position: 132
slug: /ja/integrations/hashboard
keywords: [clickhouse, hashboard, connect, integrate, ui, analytics]
description: HashboardはClickHouseと簡単に統合可能な強力な分析プラットフォームで、リアルタイムデータ分析を提供します。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_native.md';

# ClickHouseとHashboardの接続

[Hashboard](https://hashboard.com)は、組織内の誰もがメトリクスを追跡し、行動可能なインサイトを発見できるインタラクティブなデータ探索ツールです。HashboardはあなたのClickHouseデータベースに対してライブSQLクエリを発行し、セルフサービス型のアドホックなデータ探索用途に特に役立ちます。

<img src={require('./images/hashboard_01.png').default} class="image" alt="Hashboard data explorer" />

<br/>

このガイドでは、HashboardをあなたのClickHouseインスタンスに接続する手順を案内します。この情報は、Hashboardの[ClickHouseインテグレーションドキュメント](https://docs.hashboard.com/docs/database-connections/clickhouse)でもご覧いただけます。

## 前提条件

- 自身のインフラストラクチャ上または[ClickHouse Cloud](https://clickhouse.com/)上にホストされているClickHouseデータベース。
- [Hashboardアカウント](https://hashboard.com/getAccess)とプロジェクト。

## HashboardをClickHouseに接続する手順

### 1. 接続詳細の収集

<ConnectionDetails />

### 2. Hashboardで新しいデータベース接続を追加

1. [Hashboardプロジェクト](https://hashboard.com/app)に移動します。
2. サイドナビゲーションバーのギアアイコンをクリックして設定ページを開きます。
3. `+ New Database Connection`をクリックします。
4. モーダルで「ClickHouse」を選択します。
5. **Connection Name**、**Host**、**Port**、**Username**、**Password**、**Database**の各フィールドに、事前に収集した情報を入力します。
6. 「Test」をクリックして接続が正常に構成されていることを確認します。
7. 「Add」をクリックします。

これで、ClickHouseデータベースがHashboardに接続され、[Data Models](https://docs.hashboard.com/docs/data-modeling/add-data-model)、[Explorations](https://docs.hashboard.com/docs/visualizing-data/explorations)、[Metrics](https://docs.hashboard.com/docs/metrics)、[Dashboards](https://docs.hashboard.com/docs/dashboards)を構築できるようになります。これらの機能についての詳細はHashboardの対応するドキュメントを参照してください。

## 詳細情報

より高度な機能やトラブルシューティングの情報については、[Hashboardのドキュメント](https://docs.hashboard.com/)を参照してください。
