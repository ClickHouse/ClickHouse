---
slug: /ja
sidebar_label: ClickHouseとは？
description: "ClickHouse®は、オンライン分析処理（OLAP）向けの列指向SQLデータベース管理システム（DBMS）です。オープンソースソフトウェアとしても、クラウド提供としても利用可能です。"
title: ClickHouseとは？
---

ClickHouse®は、オンライン分析処理（OLAP）向けの高性能な列指向SQLデータベース管理システム（DBMS）です。これには、[オープンソースソフトウェア](https://github.com/ClickHouse/ClickHouse)としての提供と、[クラウド提供](https://clickhouse.com/cloud)の両方があります。

## 分析とは？

分析、またはOLAP（オンライン分析処理）は、大規模なデータセットに対して複雑な計算（例：集計、文字列処理、算術演算）を行うSQLクエリを指します。

トランザクションクエリ（またはOLTP、オンライン・トランザクション処理）は、1クエリあたり数行のみを読み書きし、ミリ秒で完了するのに対し、分析クエリは通常、数十億、数兆の行を処理します。

多くのユースケースでは、[分析クエリは「リアルタイム」でなければならない](https://clickhouse.com/engineering-resources/what-is-real-time-analytics)、つまり1秒未満で結果を返す必要があります。

## 行指向ストレージと列指向ストレージ

そのようなパフォーマンスレベルは、適切なデータの「指向」でのみ達成できます。

データベースはデータを行指向または列指向で格納します。

行指向データベースでは、連続するテーブル行が順次に格納されます。このレイアウトにより、行のカラム値が一緒に格納されているため、行を迅速に取得することができます。

ClickHouseは列指向データベースです。こうしたシステムでは、テーブルはカラムのコレクションとして格納されます。つまり、各カラムの値が順次に格納されます。このレイアウトは、単一の行を復元するのが困難（行の値の間にギャップがあるため）ですが、フィルタリングや集計といったカラム操作が行指向データベースよりもはるかに速くなります。

この違いは、3つのカラムでフィルタリングを行う例のクエリで最もよく説明できます：

```sql      
SELECT *  
FROM table  
WHERE time > ‘2024-09-01 13:14:15’  
AND location = ‘Berlin’  
AND `Mobile Phone` LIKE ‘%61010%`
```

**行指向DBMS**

![Row-oriented](@site/docs/ja/images/row-oriented.gif#)

**列指向DBMS**

![Column-oriented](@site/docs/ja/images/column-oriented.gif#)

## データのレプリケーションと整合性

ClickHouseは、非同期のマルチマスターレプリケーション方式を使用して、データが複数のノードに冗長的に格納されることを保証します。任意の利用可能なレプリカに書き込まれた後、残りのレプリカがバックグラウンドでコピーを取得します。システムは異なるレプリカ間で同一のデータを維持します。ほとんどの障害からの復旧は自動的に、あるいは複雑なケースでは半自動的に行われます。

## ロールベースのアクセス制御

ClickHouseは、SQLクエリを使用したユーザーアカウント管理を実装し、ANSI SQL標準や一般的なリレーショナルデータベース管理システムで見られるようなロールベースのアクセス制御設定を可能にします。

## SQLサポート

ClickHouseは、ANSI SQL標準と多くの場合同一である[SQLに基づく宣言的クエリ言語](/docs/ja/sql-reference)をサポートします。サポートされているクエリ句には、[GROUP BY](/docs/ja/sql-reference/statements/select/group-by)、[ORDER BY](/docs/ja/sql-reference/statements/select/order-by)、[FROM](/docs/ja/sql-reference/statements/select/from)でのサブクエリ、[JOIN](/docs/ja/sql-reference/statements/select/join)句、[IN](/docs/ja/sql-reference/operators/in)演算子、[ウィンドウ関数](/docs/ja/sql-reference/window-functions)、スカラーサブクエリが含まれます。

## 近似計算

ClickHouseはパフォーマンスを向上させるために正確さを犠牲にする方法を提供します。例えば、一部の集計関数は、異なる値のカウント、中央値、分位点を近似的に計算します。また、クエリはデータのサンプルに対して実行され、迅速に近似結果を計算できます。最後に、すべてのキーではなく、限られた数のキーで集計を実行できます。キーの分布の偏りによっては、正確な計算よりもはるかに少ないリソースを使用しつつ、合理的に正確な結果を提供できます。

## アダプティブ結合アルゴリズム

ClickHouseは結合アルゴリズムを適応的に選択し、最初に高速なハッシュ結合を使用し、大きなテーブルが複数ある場合にマージ結合にフォールバックします。

## 優れたクエリパフォーマンス

ClickHouseは極めて高速なクエリパフォーマンスで知られています。
ClickHouseがなぜこれほど速いのかを知るには、[ClickHouseはなぜ速いのか？](/docs/ja/concepts/why-clickhouse-is-so-fast.md)ガイドを参照してください。
