---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: "\u30D0\u30C3\u30D5\u30A1"
---

# バッファ {#buffer}

バッファのデータを書き込ram、定期的にフラッシングで別表に示す。 読み取り操作中に、バッファと他のテーブルからデータが同時に読み取られます。

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

エンジン変数:

-   `database` – Database name. Instead of the database name, you can use a constant expression that returns a string.
-   `table` – Table to flush data to.
-   `num_layers` – Parallelism layer. Physically, the table will be represented as `num_layers` 独立した緩衝の。 推奨値:16。
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`、と `max_bytes` – Conditions for flushing data from the buffer.

データはバッファーからフラッシュされ、すべてのファイルが `min*` 条件または少なくとも一つ `max*` 条件が満たされる。

-   `min_time`, `max_time` – Condition for the time in seconds from the moment of the first write to the buffer.
-   `min_rows`, `max_rows` – Condition for the number of rows in the buffer.
-   `min_bytes`, `max_bytes` – Condition for the number of bytes in the buffer.

書き込み操作中に、データはaに挿入されます。 `num_layers` ランダムバッファの数。 または、挿入するデータ部分が十分大きい場合（ `max_rows` または `max_bytes`）、バッファを省略して、宛先テーブルに直接書き込まれます。

データを洗い流すための条件はのそれぞれのために別に計算されます `num_layers` バッファ。 たとえば、次の場合 `num_layers = 16` と `max_bytes = 100000000`、最大RAM消費は1.6GBです。

例えば:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

作成する ‘merge.hits\_buffer’ 同じ構造のテーブル ‘merge.hits’ そして緩衝エンジンを使用して。 このテーブルに書き込むとき、データはRAMにバッファリングされ、後で ‘merge.hits’ テーブル。 16バッファが作成されます。 100秒が経過した場合、または百万行が書き込まれた場合、または100MBのデータが書き込まれた場合、または同時に10秒が経過し、10,000行と10MBのデータが たとえば、一つの行だけが書き込まれた場合、100秒後には何があってもフラッシュされます。 しかし、多くの行が書き込まれた場合、データはすぐにフラッシュされます。

サーバが停止し、ドテーブルやテーブル取り外し、バッファのデータも出力先にフラッシュされる。

データベース名およびテーブル名には、一重引quotationで空の文字列を設定できます。 これは、宛先テーブルがないことを示します。 この場合、データのフラッシュ条件に達すると、バッファは単純にクリアされます。 これは、データのウィンドウをメモリに保持するのに便利です。

バッファテーブルから読み取るとき、データはバッファと宛先テーブルの両方から処理されます（存在する場合）。
このバッファーとなる指数です。 つまり、データのバッファが読み取れる大きなバッファ. （下位テーブルのデータの場合、サポートするインデックスが使用されます。)

バッファテーブルの列のセットが下位テーブルの列のセットと一致しない場合、両方のテーブルに存在する列のサブセットが挿入されます。

バッファテーブルと下位テーブルのいずれかの列に型が一致しない場合、サーバーログにエラーメッセージが入力され、バッファがクリアされます。
バッファがフラッシュされたときに下位テーブルが存在しない場合も同じことが起こります。

下位テーブルとバッファテーブルに対してalterを実行する必要がある場合は、まずバッファテーブルを削除し、その下位テーブルに対してalterを実行してから

サーバーが異常に再起動されると、バッファー内のデータは失われます。

バッファテーブルでは、finalとsampleが正しく動作しません。 これらの条件は宛先テーブルに渡されますが、バッファー内のデータの処理には使用されません。 これらの特徴が必要のみを使用することをお勧めしまうバッファのテーブルを書き込み、読みの行先表に示す。

を追加する場合にデータをバッファのバッファがロックされています。 これにより、読み取り操作がテーブルから同時に実行されると遅延が発生します。

バッファテーブルに挿入されるデータは、異なる順序で異なるブロックで従属テーブルに格納される可能性があります。 このため、バッファテーブルは、collapsingmergetreeへの書き込みには正しく使用することが困難です。 問題を回避するには、次のように設定します ‘num\_layers’ 1になります。

の場合は先テーブルがそのままに再現され、期待の特徴を再現でテーブルが失われた書き込みバッファへの表に示す。 データパーツの行とサイズの順序をランダムに変更すると、データ重複排除が機能しなくなります。 ‘exactly once’ 書く再現します。

これらの欠点のために、まれにバッファテーブルの使用を推奨することができます。

バッファテーブルは、ある時間単位で多数のサーバーから大量の挿入が受信され、挿入前にデータをバッファリングできない場合に使用されます。

バッファテーブルの場合でも、一度に一行ずつデータを挿入するのは意味がないことに注意してください。 これにより、毎秒数千行の速度しか生成されませんが、より大きなデータブロックを挿入すると、毎秒百万行を生成することができます（セクションを “Performance”).

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/buffer/) <!--hide-->
