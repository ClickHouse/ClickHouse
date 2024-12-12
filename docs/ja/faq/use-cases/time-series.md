---
slug: /ja/faq/use-cases/time-series
title: ClickHouseを時系列データベースとして使用できますか？
toc_hidden: true
toc_priority: 101
---

# ClickHouseを時系列データベースとして使用できますか？ {#can-i-use-clickhouse-as-a-time-series-database}

_注: ClickHouseを使用した時系列データ分析の追加例については、ブログ [Working with Time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse) をご覧ください。_

ClickHouseは、[OLAP](../../faq/general/olap.md) ワークロード向けの汎用データストレージソリューションであり、多くの特化された時系列データベース管理システムが存在します。それにもかかわらず、ClickHouseの[クエリ実行速度への注力](../../concepts/why-clickhouse-is-so-fast.md)は、多くの場合、特化されたシステムを上回ることを可能にします。このテーマに関する独立したベンチマークはたくさんあるので、ここで行うことはありません。その代わり、あなたの用途としてそれを使用する場合に重要なClickHouseの機能に焦点を当てます。

まず、典型的な時系列として使用される**[特化されたコーデック](../../sql-reference/statements/create/table.md#specialized-codecs)**があります。`DoubleDelta`や`Gorilla`などの一般的なアルゴリズムやClickHouse特有の`T64`があります。

次に、時系列クエリはしばしば最新のデータ、例えば1日や1週間前のデータにのみアクセスします。高速なnVME/SSDドライブと大容量のHDDドライブを備えたサーバーを利用することが理にかなっています。ClickHouseの[有効期限 (TTL)](/docs/ja/engines/table-engines/mergetree-family/mergetree.md/##table_engine-mergetree-multiple-volumes) 機能を使用すると、新しいホットデータを高速ドライブに保持し、それが古くなるにつれて徐々に遅いドライブに移動するように設定できます。要件が必要とする場合には、さらに古いデータのロールアップや削除も可能です。

生データの保存と処理を推奨するClickHouseの哲学には反するものの、[Materialized View](../../sql-reference/statements/create/view.md) を使用すると、さらに厳密なレイテンシーやコスト要件に適合させることができます。

## 関連コンテンツ

- ブログ: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
