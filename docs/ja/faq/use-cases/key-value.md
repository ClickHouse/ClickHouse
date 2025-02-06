---
slug: /ja/faq/use-cases/key-value
title: ClickHouseをキー・バリューストレージとして使用できますか？
toc_hidden: true
toc_priority: 101
---

# ClickHouseをキー・バリューストレージとして使用できますか？ {#can-i-use-clickhouse-as-a-key-value-storage}

短い答えは**「いいえ」**です。キー・バリューのワークロードは、ClickHouseを使用**しない**ほうが良いケースの上位に位置しています。結局のところ、ClickHouseは[OLAP](../../faq/general/olap.md)システムであり、優れたキー・バリューストレージシステムが他にも多く存在しています。

しかし、キー・バリューのようなクエリにClickHouseを使用しても意味がある状況もあるかもしれません。通常、低予算のプロダクトで、主なワークロードは分析的でありClickHouseに適しているが、キー・バリューパターンを必要とする高いリクエストスループットが求められず、厳しい遅延要件もない二次プロセスが存在する場合です。予算が無制限であれば、この二次ワークロード用に別のキー・バリューデータベースをインストールするでしょうが、現実には、もう一つのストレージシステムを維持する追加のコスト（監視、バックアップなど）があるため、これを避けることが望ましいかもしれません。

推奨に逆らってClickHouseでキー・バリューのようなクエリを実行することを決めた場合、以下のようなヒントがあります：

- ClickHouseでポイントクエリが高コストとなる主な理由は、メインの[MergeTreeテーブルエンジンファミリー](../..//engines/table-engines/mergetree-family/mergetree.md)のスパースな主キーインデックスです。このインデックスは特定の行に直接ポイントできず、N番目ごとにポイントするため、隣接するN番目の行から目的の行までスキャンする必要があり、その過程で余分なデータを読み取ります。キー・バリューのシナリオでは、`index_granularity`設定でNの値を減らすことが有効かもしれません。
- ClickHouseは各カラムを別々のファイルセットに保持しているため、1つの完全な行を組み立てるためには各ファイルを通過する必要があります。カラム数が多いほどファイル数は線形に増加するため、キー・バリューシナリオでは多くのカラムの使用を避け、すべてのペイロードを単一の`String`カラムにJSON、Protobuf、または適切なシリアル化形式でエンコードして格納することが価値を持つかもしれません。
- 通常の`MergeTree`テーブルの代わりに[Join](../../engines/table-engines/special/join.md)テーブルエンジンと[joinGet](../../sql-reference/functions/other-functions.md#joinget)関数を使用してデータを取得する代替アプローチがあります。これによってクエリ性能が向上する可能性がありますが、使い勝手や信頼性の問題が発生する可能性があります。[この使用例](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00800_versatile_storage_join.sql#L49-L51)をご覧ください。
