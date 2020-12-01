---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 78
toc_title: "\u4E00\u822C\u7684\u306A\u8CEA\u554F"
---

# 一般的な質問 {#general-questions}

## なぜMapReduceのようなものを使わないのですか？ {#why-not-use-something-like-mapreduce}

MapReduceのようなシステムは、reduce操作が分散ソートに基づいている分散コンピューティングシステムと呼ぶことができます。 このクラ [Apache Hadoop](http://hadoop.apache.org). Yandexは社内ソリューションYTを使用しています。

これらのシステムなの適切なオンラインのクエリによる高の待ち時間をゼロにすることに つまり、webインターフェイスのバックエンドとして使用することはできません。 これらのシステムなに役立つリアルタイムデータの更新をした。 分散ソートは、操作の結果とすべての中間結果（存在する場合）が単一のサーバーのRAMにある場合、reduce操作を実行する最良の方法ではありません。 このような場合、ハッシュテーブルはreduce操作を実行する最適な方法です。 Map-reduceタスクを最適化するための一般的なアプローチは、RAM内のハッシュテーブルを使用した事前集約（部分削減）です。 ユーザーはこの最適化を手動で実行します。 分散ソートは、単純なmap-reduceタスクを実行するときのパフォーマンスの低下の主な原因の一つです。

ほとんどのMapReduce実装では、クラスター上で任意のコードを実行できます。 しかし、宣言型クエリ言語は、実験を迅速に実行するためにOLAPに適しています。 たとえば、HadoopにはHiveとPigがあります。 また、SparkのCloudera ImpalaやShark(旧式)、Spark SQL、Presto、Apache Drillについても検討してください。 このようなタスクを実行するときのパフォーマンスは、特殊なシステムに比べて非常に最適ではありませんが、比較的遅延が高いため、これらのシステ

## になっているのでしょうか？しているのでエンコーディング利用の場合OracleをODBC? {#oracle-odbc-encodings}

外部ディクショナリのソースとしてODBCドライバを介してOracleを使用する場合は、外部ディクショナリの正しい値を設定する必要が `NLS_LANG` 環境変数in `/etc/default/clickhouse`. 詳細については、を参照してください [Oracle NLS\_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**例**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## ClickHouseからファイルにデータをエクスポートするには？ {#how-to-export-to-file}

### INTO OUTFILE句の使用 {#using-into-outfile-clause}

追加 [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) クエリの句。

例えば:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

デフォルトでは、ClickHouseは [TabSeparated](../interfaces/formats.md#tabseparated) 出力データの形式。 選択するには [データ形式](../interfaces/formats.md) は、 [FORMAT句](../sql-reference/statements/select/format.md#format-clause).

例えば:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### ファイルエンジンテーブルの使用 {#using-a-file-engine-table}

見る [ファイル](../engines/table-engines/special/file.md).

### コマンドラインリダイレクト {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

見る [clickhouse-クライアント](../interfaces/cli.md).

{## [元の記事](https://clickhouse.tech/docs/en/faq/general/) ##}
