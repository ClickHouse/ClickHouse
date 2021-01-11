---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: TinyLog
---

# TinyLog {#tinylog}

エンジンはログエンジンファミリに属します。 見る [ログエンジン家族](index.md) ログエンジンの共通プロパティとその違い。

このテーブルエンジンは、通常、write-onceメソッドで使用されます。 たとえば、次のようにします `TinyLog`-小さなバッチで処理される中間データのテーブルを入力します。 多数の小さなテーブルにデータを格納するのは非効率的です。

クエリは単一のストリームで実行されます。 言い換えれば、このエンジンは比較的小さなテーブル（最大約1,000,000行）を対象としています。 小さなテーブルが多い場合は、このテーブルエンジンを使用するのが理にかなっています。 [ログ](log.md) エンジン(開く必要の少ないファイル)。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/tinylog/) <!--hide-->
