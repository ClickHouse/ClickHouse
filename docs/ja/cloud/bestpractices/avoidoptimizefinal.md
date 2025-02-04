---
slug: /ja/cloud/bestpractices/avoid-optimize-final
sidebar_label: Optimize Finalを避ける
title: Optimize Finalを避ける
---

[`OPTIMIZE TABLE ... FINAL`](/docs/ja/sql-reference/statements/optimize/) クエリを使用すると、特定のテーブルのデータパーツを1つにマージする予定外のマージが開始されます。このプロセス中、ClickHouseはすべてのデータパーツを読み込み、それらを解凍し、マージし、1つのパートに圧縮し直し、オブジェクトストアに書き戻します。これにより、多大なCPUおよびIOリソースが消費されます。

この最適化では、すでに1つのパーツにマージされている場合でも、その1つのパーツを書き換えることに注意が必要です。また、「1つのパーツ」の範囲についても重要です - これは、設定値 [`max_bytes_to_merge_at_max_space_in_pool`](https://clickhouse.com/docs/ja/operations/settings/merge-tree-settings#max-bytes-to-merge-at-max-space-in-pool) が無視されることを示します。例えば、[`max_bytes_to_merge_at_max_space_in_pool`](https://clickhouse.com/docs/ja/operations/settings/merge-tree-settings#max-bytes-to-merge-at-max-space-in-pool) はデフォルトで150 GBに設定されています。OPTIMIZE TABLE ... FINALを実行すると、残りの1つのパーツがこのサイズを超える可能性があります。150 GBの多数のパーツを1つにマージすることは、大量の時間やメモリを要する可能性があるため、このコマンドを一般的に使用しないもう一つの重要な考慮点です。
