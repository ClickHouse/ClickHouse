---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u30D0\u30C3\u30D5\u30A1"
---

# バッファ {#buffer}

バッファのデータを書き込RAM、定期的にフラッシングで別表に示す。 読み取り動作中、データはバッファと他のテーブルから同時に読み取られます。

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

エンジン変数:

-   `database` – Database name. Instead of the database name, you can use a constant expression that returns a string.
-   `table` – Table to flush data to.
-   `num_layers` – Parallelism layer. Physically, the table will be represented as `num_layers` 独立した緩衝の。 推奨値:16.
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`,and `max_bytes` – Conditions for flushing data from the buffer.

データはバッファからフラッシュされ、宛先テーブルに書き込まれます。 `min*` 条件または少なくとも一つ `max*` 条件が満たされる。

-   `min_time`, `max_time` – Condition for the time in seconds from the moment of the first write to the buffer.
-   `min_rows`, `max_rows` – Condition for the number of rows in the buffer.
-   `min_bytes`, `max_bytes` – Condition for the number of bytes in the buffer.

書き込み操作の間、データはaに挿入されます `num_layers` ランダムバッファの数。 または、挿入するデータ部分が十分に大きい（より大きい）場合 `max_rows` または `max_bytes`）、それはバッファを省略して、宛先テーブルに直接書き込まれます。

データをフラッシュするための条件は、 `num_layers` バッファ たとえば、 `num_layers = 16` と `max_bytes = 100000000`、最大RAM消費量は1.6GBです。

例:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

作成 ‘merge.hits\_buffer’ と同じ構造を持つテーブル ‘merge.hits’ そして、バッファエンジンを使用。 このテーブルに書き込むとき、データはRAMにバッファされ、後で ‘merge.hits’ テーブル。 16のバッファが作成されます。 それぞれのデータは、100秒が経過した場合、または百万行が書き込まれた場合、または100MBのデータが書き込まれた場合、または10秒が経過して10,000行と10 たとえば、ただ一つの行が書き込まれている場合、100秒後には何があってもフラッシュされます。 しかし、多くの行が書き込まれている場合、データは早くフラッシュされます。

DROP TABLEまたはDETACH TABLEを使用してサーバーを停止すると、バッファーデータも宛先テーブルにフラッシュされます。

データベース名とテーブル名には、単一引quotationで空の文字列を設定できます。 これは、宛先テーブルがないことを示します。 この場合、データフラッシュ条件に達すると、バッファは単純にクリアされます。 これは、データのウィンドウをメモリに保持する場合に便利です。

バッファテーブルから読み取るとき、データはバッファと宛先テーブルの両方から処理されます(存在する場合)。
バッファテーブルはインデックスをサポートしません。 つまり、バッファ内のデータは完全にスキャンされます。 (下位テーブルのデータについては、サポートするインデックスが使用されます。)

バッファテーブル内の列のセットが下位テーブル内の列のセットと一致しない場合、両方のテーブルに存在する列のサブセットが挿入されます。

バッファテーブル内のいずれかの列と下位テーブルで型が一致しない場合、サーバログにエラーメッセージが入力され、バッファがクリアされます。
バッファがフラッシュされたときに下位テーブルが存在しない場合も同じことが起こります。

下位テーブルとバッファーテーブルに対してALTERを実行する必要がある場合は、まずバッファーテーブルを削除し、下位テーブルに対してALTERを実行してから、バッ

サーバーが異常に再起動されると、バッファー内のデータは失われます。

最終サンプルな正常に動作しないためのバッファです。 これらの条件は宛先テーブルに渡されますが、バッファー内のデータの処理には使用されません。 これらの特徴が必要のみを使用することをお勧めしまうバッファのテーブルを書き込み、読みの行先表に示す。

を追加する場合にデータをバッファのバッファがロックされています。 これにより、テーブルから読み取り操作が同時に実行される場合に遅延が発生します。

バッファテーブルに挿入されるデータは、下位テーブル内で異なる順序と異なるブロックになる場合があります。 このため、CollapsingMergeTreeに正しく書き込むためにバッファテーブルを使用することは困難です。 問題を回避するには、以下を設定できます ‘num\_layers’ に1.

の場合は先テーブルがそのままに再現され、期待の特徴を再現でテーブルが失われた書き込みバッファへの表に示す。 行の順序とデータ部分のサイズがランダムに変更されると、データ重複排除が動作を終了します。 ‘exactly once’ 書く再現します。

これらの欠点があるため、まれにしかバッファテーブルの使用を推奨できません。

バッファテーブルは、単位時間にわたって多数のサーバーから多数の挿入を受信し、挿入の前にデータをバッファリングできない場合に使用されます。

バッファテーブルであっても、一度にデータ行を挿入することは意味がありません。 これにより、毎秒数千行の速度が生成されますが、より大きなデータブロックを挿入すると、毎秒百万行を超える速度が生成されます（セクションを参照 “Performance”).

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/buffer/) <!--hide-->
