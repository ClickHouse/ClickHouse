---
slug: /ja/updating-data
title: データの更新
description: ClickHouseでデータを更新する方法
keywords: [更新, データ更新]
---

## ClickHouseとOLTPデータベースにおけるデータ更新の違い

更新を処理する際、ClickHouseとOLTPデータベースは、その基礎となる設計理念とターゲットユースケースの違いにより大きく分かれます。例えば、PostgreSQLは行指向でACID準拠のリレーショナルデータベースであり、マルチバージョン同時実行制御（MVCC）などのメカニズムを通じて、堅牢かつトランザクション性のある更新および削除操作をサポートし、データの整合性を確保します。これにより、高い同時実行環境でも安全かつ信頼性のある修正が可能です。

一方、ClickHouseは読み込みが多い分析と高スループットの追加のみの操作に最適化された列指向のデータベースです。インプレース更新と削除をネイティブでサポートしていますが、高いI/Oを避けるため注意して使用する必要があります。別の方法として、削除や更新を非同期的または読み取り時に処理される追加操作に変換するようにテーブルを再構築することができ、リアルタイムのデータ操作よりも高スループットのデータ取り込みと効率的なクエリパフォーマンスに重点を置いています。

## ClickHouseでデータを更新する方法

ClickHouseでは、データを更新する方法がいくつかあり、それぞれに利点とパフォーマンスの特徴があります。使用するデータモデルと更新予定量に基づいて適切な方法を選択する必要があります。

どちらの操作においても、一定の時間内に処理される追加操作よりも送信された追加操作の数が絶えず上回る場合、適用されなければならない非マテリアライズされた追加操作のキューが成長し続けます。これにより、最終的に`SELECT`クエリのパフォーマンスが低下する可能性があります。

まとめると、更新操作は慎重に発行する必要があり、`system.mutations`テーブルを使用して追加操作のキューを注意深く追跡する必要があります。OLTPデータベースのように頻繁に更新を行わないでください。頻繁に更新する必要がある場合は、[ReplacingMergeTree](/ja/engines/table-engines/mergetree-family/replacingmergetree)を参照してください。

| 方法 | 構文 | 使用時期 |
| --- | --- | --- |
| [物理更新](/ja/sql-reference/statements/alter/update) | `ALTER TABLE [table] UPDATE` | データをディスクに即時更新する必要がある場合（例：コンプライアンスのため）。`SELECT`パフォーマンスに悪影響を与えます。 |
| [論理更新](/ja/guides/developer/lightweight-update) | `ALTER TABLE [table] UPDATE` | `SET apply_mutations_on_fly = 1;`を使用して有効化。少量のデータを更新する場合に使用します。更新されたデータが即座に次のすべての`SELECT`クエリで返されますが、最初はディスク上で内部的に更新済みとしてマークされるだけです。 |
| [ReplacingMergeTree](/ja/engines/table-engines/mergetree-family/replacingmergetree) | `ENGINE = ReplacingMergeTree` | 大量のデータを更新する場合に使用します。このテーブルエンジンはマージ時のデータ重複削除に最適化されています。 |

以下は、ClickHouseでデータを更新するさまざまな方法の概要です：

## 物理更新

物理更新は、例えば`ALTER TABLE … UPDATE`コマンドを通じて発行できます。

```sql
ALTER TABLE posts_temp
	(UPDATE AnswerCount = AnswerCount + 1 WHERE AnswerCount = 0)
```
これらは非常にI/Oヘビーであり、`WHERE`式に一致するすべてのパーツを再書き込みします。このプロセスには原子性がなく、準備が整い次第、変更されたパーツが置き換えられ、追加操作中に実行される`SELECT`クエリは既に変更されたパーツとまだ変更されていないパーツの両方からデータを表示します。ユーザーは[systems.mutations](/ja/operations/system-tables/mutations#system_tables-mutations)テーブルを通じて進捗状況を追跡することができます。これらはI/Oを多く消費する操作であり、クラスタの`SELECT`パフォーマンスに影響を与える可能性があるため、節度を持って使用すべきです。

[物理更新](/ja/sql-reference/statements/alter/update)について詳しく読む。

## 論理更新 (ClickHouseクラウドでのみ利用可能)

論理更新は、行を即座に更新し、その後の`SELECT`クエリで自動的に変更された値を返すメカニズムを提供します（これはオーバーヘッドを伴い、クエリを遅くします）。これにより、通常の追加操作の原子性制限が効果的に解決されます。以下に例を示します：

```sql
SET apply_mutations_on_fly = 1;

SELECT ViewCount
FROM posts
WHERE Id = 404346

┌─ViewCount─┐
│ 	26762 │
└───────────┘

1 row in set. Elapsed: 0.115 sec. Processed 59.55 million rows, 238.25 MB (517.83 million rows/s., 2.07 GB/s.)
Peak memory usage: 113.65 MiB.

-increment count 
ALTER TABLE posts
	(UPDATE ViewCount = ViewCount + 1 WHERE Id = 404346)

SELECT ViewCount
FROM posts
WHERE Id = 404346

┌─ViewCount─┐
│ 	26763 │
└───────────┘

1 row in set. Elapsed: 0.149 sec. Processed 59.55 million rows, 259.91 MB (399.99 million rows/s., 1.75 GB/s.)
```

論理更新では、データを更新するために追加操作が使用されることに注意してください。これは即座にマテリアライズされず、`SELECT`クエリの際に適用されます。バックグラウンドで非同期的に適用されるため、通常の追加操作と同様に重いオーバーヘッドがかかり、I/Oを多く消費する操作であり、節度を持って使用すべきです。この操作で使用できる式にも制限があります（[詳細](/ja/guides/developer/lightweight-update#support-for-subqueries-and-non-deterministic-functions)を参照）。

[論理更新](/ja/guides/developer/lightweight-update)について詳しく読む。

## 追加資料

- [ClickHouseにおける更新と削除の処理](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)
