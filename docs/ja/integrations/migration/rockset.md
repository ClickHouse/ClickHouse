---
title: Rocksetからの移行
slug: /ja/migrations/rockset
description: RocksetからClickHouseへの移行
keywords: [migrate, migration, migrating, data, etl, elt, rockset]
---

# Rocksetからの移行

Rocksetはリアルタイム分析データベースであり、[2024年6月にOpenAIに買収されました](https://rockset.com/blog/openai-acquires-rockset/)。
ユーザーは2024年9月30日午後5時PDTまでに[サービスからオフボードする](https://docs.rockset.com/documentation/docs/faq)必要があります。

私たちは、ClickHouse CloudがRocksetユーザーにとって素晴らしい選択肢となると考えています。このガイドでは、RocksetからClickHouseに移行する際に考慮すべきポイントについて説明します。

では、始めましょう！

## 即時のサポート

即時のサポートが必要な場合は、[このフォーム](https://clickhouse.com/company/contact?loc=docs-rockest-migrations)にご記入いただき、人間の担当者が対応いたします！


## ClickHouseとRocksetの比較 - 概要

まず、ClickHouseの強みと、Rocksetとの比較で期待される利点について簡単に説明します。

ClickHouseは、スキーマファーストアプローチを通じてリアルタイムパフォーマンスとコスト効率に焦点を当てています。
半構造化データもサポートしていますが、ユーザーはデータの構造をどのように設定するかを決定することで、性能とリソースの効率を最大化するという哲学を持っています。
上記のスキーマファーストアプローチの結果として、ベンチマークでは、ClickHouseはRocksetを上回るスケーラビリティ、インジェスショントラフプット、クエリパフォーマンス、およびコスト効率を実現しています。

他のデータシステムとの統合に関しては、ClickHouseにはRocksetを超える[広範な機能](/ja/integrations)があります。

RocksetとClickHouseの両方は、クラウドベースの製品と関連するサポートサービスを提供しています。
Rocksetとは異なり、ClickHouseにはオープンソース製品とコミュニティがあります。
ClickHouseのソースコードは[github.com/clickhouse/clickhouse](https://github.com/clickhouse/clickhouse)で見つけることができ、執筆時点で1,500人を超える貢献者がいます。
[ClickHouse Community Slack](https://clickhouse.com/slack)には、7,000人以上のメンバーが経験やベストプラクティスを共有し、問題に対する支援をしています。

この移行ガイドはRocksetからClickHouse Cloudへの移行に焦点を当てていますが、ユーザーはオープンソース機能に関する[その他のドキュメント](/)も参照できます。

## Rocksetのキーポイント

まず、[Rocksetのキーポイント](https://docs.rockset.com/documentation/docs/key-concepts)を見て、ClickHouse Cloudでの同等の機能（存在する場合）を説明します。

### データソース

RocksetとClickHouseの両方が様々なソースからデータのロードをサポートしています。

Rocksetでは、データソースを作成し、そのデータソースに基づいて_コレクション_を作成します。
イベントストリーミングプラットフォーム、OLTPデータベース、クラウドバケットストレージに対する完全管理された統合があります。

ClickHouse Cloudでは、完全管理された統合の同等物は[ClickPipes](/ja/integrations/ClickPipes)です。
ClickPipesはイベントストリーミングプラットフォームとクラウドバケットストレージから継続的にデータをロードすることをサポートしています。
ClickPipesはデータを_テーブル_にロードします。

### インジェスト変換

Rocksetのインジェスト変換は、Rocksetに入ってくる生データをコレクションに保存する前に変換することができます。
ClickHouse CloudでもClickPipesを通じて同様のことができ、ClickHouseの[Materialized View機能](/ja/guides/developer/cascading-materialized-views)を使用してデータを変換します。

### コレクション

Rocksetでは、クエリをコレクションに対して実行します。ClickHouse Cloudでは、クエリをテーブルに対して実行します。
どちらのサービスでも、クエリはSQLを使用して実行されます。
ClickHouseは、データを操作および変換するためのSQL標準の関数に加えて、さらなる関数を追加しています。

### クエリ ラムダ

Rocksetは、専用のRESTエンドポイントから実行できる名前付きのパラメータ化されたクエリであるクエリ ラムダをサポートしています。
ClickHouse Cloudの[クエリAPIエンドポイント](/ja/get-started/query-endpoints)は同様の機能を提供しています。

### ビュー

Rocksetでは、SQLクエリで定義された仮想的なコレクションであるビューを作成できます。
ClickHouse Cloudはさまざまな[ビュー](/ja/sql-reference/statements/create/view)をサポートしています：

* _通常のビュー_ はデータを保持しません。クエリ時に別のテーブルからの読み取りを行います。
* _パラメータ化されたビュー_ は通常のビューと似ていますが、クエリ時に解決されるパラメータと共に作成できます。
* _Materialized View_ は対応する`SELECT`クエリによって変換されたデータを格納します。関連するソースデータに新しいデータが追加されたときにトリガーとして機能します。

### エイリアス

Rocksetのエイリアスは、複数の名前をコレクションに関連付けるために使用されます。
ClickHouse Cloudでは、同等の機能はサポートされていません。

### ワークスペース

Rocksetのワークスペースは、リソース（例：コレクション、クエリラムダ、ビュー、およびエイリアス）および他のワークスペースを保持するコンテナです。

ClickHouse Cloudでは、完全な分離を実現するために異なるサービスを利用できます。
また、異なるテーブル/ビューへのRBACアクセスを簡素化するためにデータベースを作成することもできます。

## 設計上の考慮事項

このセクションでは、Rocksetのキーフィーチャーをいくつか確認し、ClickHouse Cloudを使用する際の対応方法を学びます。

### JSONサポート

Rocksetは、Rockset固有の型をサポートする拡張版JSON形式をサポートしています。

ClickHouseでJSONを操作するための方法はいくつかあります：

* JSON推論
* クエリ時のJSON抽出
* インサート時のJSON抽出

ユーザーケースに最適なアプローチを理解するには、[JSONに関するドキュメント](/docs/ja/integrations/data-formats/json)をご覧ください。

さらに、ClickHouseにはまもなく[半構造化カラムデータ型](https://github.com/ClickHouse/ClickHouse/issues/54864)が追加されます。
この新しい型は、RocksetのJSON型が提供する柔軟性をユーザーに提供するはずです。

### フルテキスト検索

Rocksetは`SEARCH`関数でフルテキスト検索をサポートしています。
ClickHouseは検索エンジンではありませんが、[文字列検索用のさまざまな関数](/ja/sql-reference/functions/string-search-functions)を持っています。
ClickHouseはまた、多くのシナリオで役立つ[ブルームフィルター](/ja/optimize/skipping-indexes)をサポートしています。

### ベクトル検索

Rocksetには類似度インデックスがあり、ベクトル検索アプリケーションで使用される埋め込みをインデックスするために使用できます。

ClickHouseも線形スキャンを使用してベクトル検索を実行できます：
- [ClickHouseでのベクトル検索 - パート1](https://clickhouse.com/blog/vector-search-clickhouse-p1?loc=docs-rockest-migrations)
- [ClickHouseでのベクトル検索 - パート2](https://clickhouse.com/blog/vector-search-clickhouse-p2?loc=docs-rockest-migrations)

ClickHouseには[ベクトル検索類似度インデックス](https://clickhouse.com/docs/ja/engines/table-engines/mergetree-family/annindexes)もありますが、このアプローチは現在実験中であり、[新しいクエリアナライザー](https://clickhouse.com/docs/ja/guides/developer/understanding-query-execution-with-the-analyzer)とはまだ互換性がありません。

### OLTPデータベースからのデータのインジェスト

Rocksetの管理された統合は、MongoDBやDynamoDBなどのOLTPデータベースからのデータのインジェストをサポートしています。

DynamoDBからデータをインジェストする場合は、[こちらのDynamoDB統合ガイド](/docs/ja/integrations/data-ingestion/dbms/dynamodb/index.md)を参照してください。

### コンピュート・コンピュート分離

コンピュート・コンピュート分離は、リアルタイム分析システムにおけるアーキテクチャデザインパターンで、突然のデータ流入やクエリに対応できるようにするものです。
単一のコンポーネントがインジェストとクエリの両方を処理する場合、クエリの急増があるとインジェスト待機時間が増加し、データ流入が急増するとクエリ待機時間が増加します。

コンピュート・コンピュート分離は、この問題を回避するためにデータインジェストとクエリ処理のコードパスを分離します。この機能は2023年3月にRocksetで実装されました。

この機能は現在ClickHouse Cloudで実装中であり、プライベートプレビューに近づいています。サポートに連絡して有効化してください。

## 無料移行サービス

Rocksetユーザーにとってストレスの多い時期であることを私たちは理解しています - 誰も短期間で本番データベースを移行したいとは思わないでしょう！

ClickHouseがあなたに適しているかもしれない場合、移行をスムーズに行うために[無料移行サービス](https://clickhouse.com/comparison/rockset?loc=docs-rockest-migrations)を提供しています。
