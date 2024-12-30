---
slug: /ja/engines/table-engines/mergetree-family/
sidebar_position: 10
sidebar_label: MergeTree ファミリー
---

# MergeTree エンジンファミリー

MergeTree ファミリーのテーブルエンジンは、ClickHouse のデータストレージ機能の中核を成しています。これらはカラム型ストレージ、カスタムパーティショニング、スパースな主キーインデックス、データスキップ用のセカンダリインデックスなど、回復力と高性能データ取得のためのほとんどの機能を提供します。

基本的な [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) テーブルエンジンは、単一ノードの ClickHouse インスタンスにおけるデフォルトのテーブルエンジンと見なせます。これは汎用性が高く、多くのユースケースに対して実用的です。

実運用では、[ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) を使用するのが一般的です。これは、通常の MergeTree エンジンのすべての機能に高可用性を追加します。さらにデータ挿入時の自動データ重複排除も行うため、挿入中にネットワークの問題があってもソフトウェアが安全に再試行できます。

MergeTree ファミリーの他のエンジンは、特定のユースケースに対して追加機能を提供します。通常、これはバックグラウンドでの追加データ操作として実装されます。

MergeTree エンジンの主な欠点は、それが比較的重いことです。そのため、典型的なパターンは、それほど多くのテーブルを持たないことです。例えば、一時データ用に多くの小さなテーブルが必要な場合は、[Log エンジンファミリー](../../../engines/table-engines/log-family/index.md)を検討してください。
