---
description: 'データをRAMにバッファし、定期的に別のテーブルへフラッシュして書き出します。読み取り時には、バッファと別のテーブルの両方から同時にデータが読み取られます。'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'Bufferテーブルエンジン'
doc_type: 'reference'
---

データをRAMにバッファし、定期的に別のテーブルへフラッシュして書き出します。読み取り時には、バッファと別のテーブルの両方から同時にデータが読み取られます。

:::note
Bufferテーブルエンジンの推奨される代替手段は、[非同期挿入](/ja/guides/best-practices/asyncinserts.md)を有効にすることです。
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### エンジンパラメータ
</div>

<div id="database">
  #### `database`
</div>

`database` – データベース名。`currentDatabase()` または文字列を返す他の定数式を使用できます。

<div id="table">
  #### `table`
</div>

`table` – データのフラッシュ先のテーブル。

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – 並列度のレイヤー。物理的には、テーブルは互いに独立した `num_layers` 個のバッファとして表されます。

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes`
</div>

バッファからデータをフラッシュする条件。

<div id="optional-engine-parameters">
  ### オプションのエンジンパラメータ
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, and `flush_bytes`
</div>

バックグラウンドでバッファ内のデータをフラッシュする条件です (省略されているか 0 の場合は、`flush*` パラメータが指定されていないことを意味します) 。

すべての `min*` 条件が満たされるか、少なくとも 1 つの `max*` 条件が満たされると、データはバッファからフラッシュされて宛先テーブルに書き込まれます。

また、少なくとも 1 つの `flush*` 条件が満たされると、バックグラウンドでフラッシュが開始されます。これは `max*` とは異なり、`flush*` を使うと、Buffer テーブルへの `INSERT` クエリにレイテンシを追加しないよう、バックグラウンドでのフラッシュを個別に設定できます。

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time`, and `flush_time`
</div>

バッファへの最初の書き込みからの経過時間 (秒) に関する条件です。

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

バッファ内の行数に関する条件。

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

バッファ内のバイト数に関する条件です。

書き込み時には、データは1つ以上のランダムなバッファ (`num_layers` で設定) に挿入されます。あるいは、挿入する データパーツ が十分に大きい場合 (`max_rows` または `max_bytes` を超える場合) は、バッファを介さずに宛先テーブルへ直接書き込まれます。

データのフラッシュ条件は、`num_layers` の各バッファごとに個別に計算されます。たとえば、`num_layers = 16`、`max_bytes = 100000000` の場合、最大 RAM 消費量は 1.6 GB です。

例:

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

`merge.hits` と同じ構造を持ち、Buffer engine を使用する `merge.hits_buffer` テーブルを作成します。このテーブルに書き込むと、データは RAM にバッファされ、後で &#39;merge.hits&#39; テーブルに書き込まれます。単一のバッファが作成され、次のいずれかの条件を満たすとデータがフラッシュされます。

* 前回のフラッシュから 100 秒が経過した場合 (`max_time`)、または
* 100 万行が書き込まれた場合 (`max_rows`)、または
* 100 MB のデータが書き込まれた場合 (`max_bytes`)、または
* 10 秒が経過し (`min_time`)、かつ 10,000 行 (`min_rows`) と 10 MB (`min_bytes`) のデータが書き込まれた場合

たとえば、1 行しか書き込まれていなくても、100 秒後には必ずフラッシュされます。一方、多数の行が書き込まれている場合は、より早くフラッシュされます。

サーバーの停止時や、`DROP TABLE` または `DETACH TABLE` の実行時にも、バッファ内のデータは宛先テーブルにフラッシュされます。

データベース名とテーブル名には、シングルクォーテーションで囲んだ空文字列を設定できます。これは宛先テーブルが存在しないことを示します。この場合、データのフラッシュ条件に達すると、バッファは単にクリアされます。これは、データのウィンドウをメモリ内に保持するのに役立つ場合があります。

Buffer テーブルを読み取るときは、バッファと宛先テーブル (存在する場合) の両方からデータが処理されます。
Buffer テーブルは索引をサポートしないことに注意してください。つまり、バッファ内のデータは全件走査されるため、バッファが大きいと低速になる可能性があります。 (従属テーブル内のデータについては、そのテーブルがサポートする索引が使用されます。)

Buffer テーブルのカラムの集合が従属テーブルのカラムの集合と一致しない場合は、両方のテーブルに存在するカラムの部分集合が挿入されます。

Buffer テーブルと従属テーブルのいずれかのカラムで型が一致しない場合、エラーメッセージがサーバーログに記録され、バッファはクリアされます。
バッファがフラッシュされる時点で従属テーブルが存在しない場合も同様です。

:::note
2021 年 10 月 26 日より前のリリースでは、Buffer テーブルに対して ALTER を実行すると `Block structure mismatch` エラーが発生します ([#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) および [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565) を参照) 。そのため、Buffer テーブルを削除して再作成する以外に方法はありません。Buffer テーブルで ALTER を実行する前に、このエラーが使用中のリリースで修正されていることを確認してください。
:::

サーバーが異常終了した場合、バッファ内のデータは失われます。

`FINAL` と `SAMPLE` は Buffer テーブルでは正しく動作しません。これらの条件は宛先テーブルには渡されますが、バッファ内のデータ処理には使用されません。これらの機能が必要な場合は、Buffer テーブルは書き込み専用として使用し、読み取りは宛先テーブルからのみ行うことを推奨します。

Buffer テーブルにデータを追加するときは、いずれかのバッファがロックされます。このため、同時にそのテーブルから読み取り操作が行われていると遅延が発生します。

Buffer テーブルに挿入されたデータは、従属テーブルでは異なる順序や異なるブロックで格納されることがあります。このため、CollapsingMergeTree への書き込みに Buffer テーブルを正しく使うのは困難です。問題を避けるには、`num_layers` を 1 に設定できます。

宛先テーブルがレプリケートされている場合、Buffer テーブルへの書き込みでは、レプリケートテーブルに期待される特性の一部が失われます。行の順序やデータパーツのサイズがランダムに変化することでデータの deduplication が機能しなくなり、その結果、レプリケートテーブルに対して信頼できる &#39;exactly once&#39; 書き込みを行えなくなります。

これらの欠点があるため、Buffer テーブルの使用を推奨できるのはまれなケースに限られます。

Buffer テーブルは、短時間に多数のサーバーから大量の INSERT を受信し、挿入前にデータをバッファリングできない、つまり INSERT を十分な速度で実行できない場合に使用されます。

Buffer テーブルであっても、データを1行ずつ insert するのは意味がないことに注意してください。これでは毎秒数千行程度しか出ませんが、より大きなデータのブロックを insert すれば、毎秒100万行を超えることもあります。