---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 78
toc_title: "\u4E00\u822C\u7684\u306A\u8CEA\u554F"
---

# 一般的な質問 {#general-questions}

## なぜmapreduceのようなものを使わないのですか？ {#why-not-use-something-like-mapreduce}

MapReduceのようなシステムは、reduce操作が分散ソートに基づいている分散計算システムとして参照することができます。 このクラ [Apache Hadoop](http://hadoop.apache.org). Yandexのは、その社内ソリューション、YTを使用しています。

これらのシス つまり、webインターフェイスのバックエンドとして使用することはできません。 これらのシステムなに役立つリアルタイムデータの更新をした。 分散ソートは、操作の結果とすべての中間結果（存在する場合）が単一のサーバーのramにある場合、reduce操作を実行する最善の方法ではありません。 このような場合、ハッシュテーブルはreduce操作を実行するのに最適な方法です。 map-reduceタスクを最適化する一般的なアプローチは、ramのハッシュテーブルを使用した事前集約(部分削減)です。 ユーザーはこの最適化を手動で実行します。 分散ソートは、単純なmap-reduceタスクを実行するときのパフォーマンス低下の主な原因の一つです。

ほとんどのmapreduce実装では、クラスター上で任意のコードを実行できます。 しかし、宣言的なクエリ言語は、実験を迅速に実行するためにolapに適しています。 たとえば、hadoopにはhiveとpigがあります。 また、sparkのためのcloudera impalaまたはshark（旧式）、spark sql、presto、およびapache drillも検討してください。 このようなタスクを実行するときのパフォーマンスは、特殊なシステムに比べて非常に低いですが、比較的待ち時間が長いため、これらのシステムを

## ORACLEをODBC経由で使用するときにエンコードに問題がある場合はどうなりますか？ {#oracle-odbc-encodings}

外部辞書のソースとしてodbcドライバーを使用してoracleを使用する場合は、正しい値を設定する必要があります。 `NLS_LANG` の環境変数 `/etc/default/clickhouse`. 詳細については、 [Oracle NLS\_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**例えば**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## ClickHouseからファイルにデータをエクスポートするには？ {#how-to-export-to-file}

### INTO OUTFILE句の使用 {#using-into-outfile-clause}

追加 [INTO OUTFILE](../query_language/select/#into-outfile-clause) クエリへの句。

例えば:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

デフォルトでは、clickhouseは [タブ区切り](../interfaces/formats.md#tabseparated) 出力データの形式。 を選択する [データ形式](../interfaces/formats.md)、を使用 [フォーマット句](../query_language/select/#format-clause).

例えば:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### ファイルエンジンテーブルの使用 {#using-a-file-engine-table}

見る [ファイル](../engines/table-engines/special/file.md).

### コマンドラインのリダイ {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

見る [クリックハウス-顧客](../interfaces/cli.md).

{## [元の記事](https://clickhouse.tech/docs/en/faq/general/) ##}
