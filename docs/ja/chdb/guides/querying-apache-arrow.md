---
title: chDBでApache Arrowをクエリする方法
sidebar_label: Apache Arrowをクエリする
slug: /ja/chdb/guides/apache-arrow
description: このガイドでは、chDBを使用してApache Arrowテーブルをクエリする方法を学びます。
keywords: [chdb, apache-arrow]
---

[Apache Arrow](https://arrow.apache.org/) は、データコミュニティで人気を博している標準化された列指向メモリフォーマットです。このガイドでは、`Python`テーブル関数を使用してApache Arrowをクエリする方法を学びます。

## セットアップ

まず、仮想環境を作成します：

```bash
python -m venv .venv
source .venv/bin/activate
```

次に、chDBをインストールします。バージョン2.0.2以上であることを確認してください：

```bash
pip install "chdb>=2.0.2"
```

次に、pyarrow、pandas、ipythonをインストールします：

```bash
pip install pyarrow pandas ipython
```

この後のガイドで使用するコマンドを実行するために、`ipython`を使用します。起動するには以下を実行してください：

```bash
ipython
```

また、Pythonスクリプトや好きなノートブックでもコードを使用できます。

## ファイルからApache Arrowテーブルを作成する

まず、[AWS CLIツール](https://aws.amazon.com/cli/)を使用して、[Ooklaデータセット](https://github.com/teamookla/ookla-open-data)のParquetファイルの1つをダウンロードします：

```bash
aws s3 cp \
  --no-sign \
  s3://ookla-open-data/parquet/performance/type=mobile/year=2023/quarter=2/2023-04-01_performance_mobile_tiles.parquet .
```

:::note
ファイルをもっとダウンロードしたい場合は、`aws s3 ls`を使ってすべてのファイルのリストを取得し、上記のコマンドを更新してください。
:::

次に、pyarrowパッケージからParquetモジュールをインポートします：

```python
import pyarrow.parquet as pq
```

そして、ParquetファイルをApache Arrowテーブルに読み込むことができます：

```python
arrow_table = pq.read_table("./2023-04-01_performance_mobile_tiles.parquet")
```

スキーマは以下の通りです：

```python
arrow_table.schema
```

```text
quadkey: string
tile: string
tile_x: double
tile_y: double
avg_d_kbps: int64
avg_u_kbps: int64
avg_lat_ms: int64
avg_lat_down_ms: int32
avg_lat_up_ms: int32
tests: int64
devices: int64
```

`shape`属性を呼び出すことで行とカラムの数を取得できます：

```python
arrow_table.shape
```

```text
(3864546, 11)
```

## Apache Arrowをクエリする

次に、chDBからArrowテーブルをクエリしましょう。まず、chDBをインポートします：

```python
import chdb
```

そして、テーブルを記述します：

```python
chdb.query("""
DESCRIBE Python(arrow_table)
SETTINGS describe_compact_output=1
""", "DataFrame")
```

```text
               name     type
0           quadkey   String
1              tile   String
2            tile_x  Float64
3            tile_y  Float64
4        avg_d_kbps    Int64
5        avg_u_kbps    Int64
6        avg_lat_ms    Int64
7   avg_lat_down_ms    Int32
8     avg_lat_up_ms    Int32
9             tests    Int64
10          devices    Int64
```

行の数もカウントできます：

```python
chdb.query("SELECT count() FROM Python(arrow_table)", "DataFrame")
```

```text
   count()
0  3864546
```

では、もう少し興味深いことをしましょう。以下のクエリは、`quadkey`および`tile.*`カラムを除外し、残りのすべてのカラムの平均と最大値を計算します：

```python
chdb.query("""
WITH numericColumns AS (
  SELECT * EXCEPT ('tile.*') EXCEPT(quadkey)
  FROM Python(arrow_table)
)
SELECT * APPLY(max), * APPLY(avg) APPLY(x -> round(x, 2))
FROM numericColumns
""", "Vertical")
```

```text
Row 1:
──────
max(avg_d_kbps):                4155282
max(avg_u_kbps):                1036628
max(avg_lat_ms):                2911
max(avg_lat_down_ms):           2146959360
max(avg_lat_up_ms):             2146959360
max(tests):                     111266
max(devices):                   1226
round(avg(avg_d_kbps), 2):      84393.52
round(avg(avg_u_kbps), 2):      15540.4
round(avg(avg_lat_ms), 2):      41.25
round(avg(avg_lat_down_ms), 2): 554355225.76
round(avg(avg_lat_up_ms), 2):   552843178.3
round(avg(tests), 2):           6.31
round(avg(devices), 2):         2.88
```
