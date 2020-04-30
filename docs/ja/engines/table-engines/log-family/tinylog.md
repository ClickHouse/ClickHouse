---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 34
toc_title: TinyLog
---

# TinyLog {#tinylog}

エンジンはログエンジンファミリに属します。 見る [丸太エンジン家族](log-family.md) ログエンジンとその違いの一般的な特性のために。

このテーブルエンジンは、通常、write-onceメソッドで使用されます。 たとえば、次のものを使用できます `TinyLog`-小さなバッチで処理される中間データのテーブルを入力します。 多数の小さなテーブルにデータを格納することは非効率的です。

クエリは単一のストリームで実行されます。 言い換えれば、このエンジンは比較的小さなテーブル（約1,000,000行まで）を対象としています。 小さなテーブルがたくさんある場合は、このテーブルエンジンを使用するのが理にかなっています。 [ログ](log.md) エンジン（少数のファイルは開く必要があります）。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/tinylog/) <!--hide-->
