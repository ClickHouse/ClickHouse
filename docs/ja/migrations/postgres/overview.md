---
slug: /ja/migrations/postgresql/overview
title: PostgreSQLからClickHouseへの移行
description: PostgreSQLからClickHouseへの移行ガイド
keywords: [postgres, postgresql, 移行, マイグレーション]
---

## なぜPostgresよりClickHouseを使用するのか？

TLDR: ClickHouseはOLAPデータベースとして、特に`GROUP BY`クエリを用いた高速分析用に設計されており、Postgresはトランザクションワークロード向けのOLTPデータベースとして設計されています。

OLTP、すなわちオンライン・トランザクション処理データベースは、トランザクション情報を管理するように設計されています。Postgresが典型的な例であるこれらのデータベースの主な目的は、エンジニアがデータベースに対して更新ブロックを送り込み、それが全体として成功するか失敗するかを保証することです。これらのタイプのトランザクション保証は、ACID特性を持つOLTPデータベースの主な焦点であり、Postgresの大きな強みです。これらの要件を考慮すると、OLTPデータベースは通常、大規模なデータセットに対する分析クエリの実行時に性能の限界に達します。

OLAP、すなわちオンライン分析処理データベースは、分析ワークロードを管理するために設計されています。これらのデータベースの主な目的は、エンジニアが膨大なデータセットに対して効率的にクエリを実行し、集計できるようにすることです。ClickHouseのようなリアルタイムOLAPシステムは、データがリアルタイムで取り込まれる際にこの分析を可能にします。

より詳細な比較については、[こちらのブログ投稿](https://clickhouse.com/blog/adding-real-time-analytics-to-a-supabase-application)をご覧ください。

ClickHouseとPostgresの分析クエリにおける性能の違いを確認するには、[PostgreSQLクエリをClickHouseで書き換える](/ja/migrations/postgresql/rewriting-queries)を参照してください。

---

**[PostgreSQL移行ガイドをここから始める](/ja/migrations/postgresql/dataset)。**
