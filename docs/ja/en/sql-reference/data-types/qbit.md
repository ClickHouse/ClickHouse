---
description: '近似ベクトル検索向けのきめ細かな量子化を可能にする ClickHouse の QBit データ型のドキュメント'
keywords: ['qbit', 'data type']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'QBit データ型'
doc_type: 'reference'
---

`QBit` データ型は、より高速な近似検索を実現するために、ベクトルの保存方式を再編成します。各ベクトルの要素をまとめて保存するのではなく、すべてのベクトルにまたがって同じ2進数のビット位置ごとにまとめて格納します。
これにより、ベクトルを完全な精度で保持したまま、検索時にきめ細かな量子化レベルを選択できます。読み取るビット数を減らせば I/O と計算量を抑えて高速化でき、多く読み取れば精度を高められます。つまり、量子化によるデータ転送量と計算量の削減による高速化の恩恵を受けつつ、必要に応じて元のデータをそのまま利用できます。

`QBit` 型のカラムを宣言するには、次の構文を使用します。

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – 各ベクトル要素の型。指定できる型は `Int8`、`BFloat16`、`Float32`、`Float64` です
* `dimension` – 各ベクトルの要素数
* `stride` – 任意。1つのストリームグループにまとめて格納される次元数です。省略した場合、デフォルトは `dimension` (単一グループ) です。指定する場合、`dimension` は `stride` の倍数である必要があります。また、`stride` が `dimension` より小さい場合、`stride` は 8 の倍数である必要があります。[Strides](#strides) を参照してください。

<div id="creating-qbit">
  ## QBit の作成
</div>

テーブルのカラム定義では `QBit` 型を使用します:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## ArrayをQBitに変換する
</div>

Arrayの長さが`QBit`の次元と一致していれば、Arrayは`QBit`に変換されます。Arrayの要素型は`QBit`の要素型と一致している必要はありません。数値型の要素であれば、どの型でも自動的に変換されます。これにより、既存の埋め込みカラムをそのまま`QBit`カラムに移行できます。

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

この変換は、`CAST` を使って明示的に行うこともできます。たとえば、`CAST(embedding AS QBit(Float32, 8))` のように指定します。

<div id="converting-qbit-to-arrays">
  ## QBit を Array に変換する
</div>

この逆変換では、ビット転置された表現から元のベクトルを復元するため、`QBit` を `Array` にキャストすると格納されている値が返されます。これは [ `Array` を `QBit` に変換する](#converting-arrays-to-qbit) の逆です。

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

再構築された配列では `QBit` の要素型が使用され、その後、各要素が要求された配列の要素型に変換されます。したがって、`QBit(Float32, N)` から `Array(Float64)` のように要素型も変更するキャストも機能します。

`Array` -&gt; `QBit` -&gt; `Array` のラウンドトリップは、`Int8`、`Float32`、`Float64` では情報の損失がありません。`BFloat16` の場合は `BFloat16` への直接変換と同じ結果になり、失われる精度は `BFloat16` 自体の精度だけです。

`dimension` が 8 の倍数でない場合、内部表現に含まれる末尾のパディング要素は取り除かれるため、結果には常にちょうど `dimension` 個の要素が含まれます。

<div id="qbit-subcolumns">
  ## QBit のサブカラム
</div>

`QBit` はサブカラムのアクセスパターンを実装しており、格納されたベクトルの個々のビットプレーンにアクセスできます。各ビット位置には、`.N` 構文 (`N` はビット位置) でアクセスできます。

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

アクセス可能なサブカラム数は、要素型 (また、ストライド化されている場合はストライドグループ数。詳細は [Strides](#strides) を参照) によって異なります。

* `Int8`: ストライドグループごとに 8 個のサブカラム (1-8)
* `BFloat16`: ストライドグループごとに 16 個のサブカラム (1-16)
* `Float32`: ストライドグループごとに 32 個のサブカラム (1-32)
* `Float64`: ストライドグループごとに 64 個のサブカラム (1-64)

<div id="strides">
  ## ストライド
</div>

デフォルトでは、`QBit` は各ビットプレーンを、すべての `dimension` 次元にまたがる単一ストリームとして格納するため、検索時には常にベクトル全体のビットプレーンを読み取ることになります。オプションの `stride` パラメータは、`dimension` 次元を `dimension / stride` 個の連続するグループに分割し、各グループのビットプレーンを別々のストリームに格納します。これにより、先頭の `D` 次元だけを対象とする検索 (`D` は `stride` の倍数) では、それらの次元を含むグループのストリームだけを読み取ればよくなります。これは、先頭の次元が実用的な低次元の埋め込みを形成する [Matryoshka embeddings](https://arxiv.org/abs/2205.13147) で有効です。

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

ここでは、4096 個の次元が 1024 ずつ 4 つのグループに分割されています。サブカラムはグループ優先順に従います。`BFloat16` (16 個の ビットプレーン) の場合、`vec.1` … `vec.16` は最初のストライドグループ (次元 1–1024) の 16 個の ビットプレーン で、`vec.17` … `vec.32` は 2 番目のグループ (次元 1025–2048) に属し、以下同様です。一般に、`vec.N` はストライドグループ `(N-1) / element_size` の ビットプレーン `(N-1) % element_size` を読み出します。

次元数を減らした検索を実行するには、読み取る次元数を転置された距離関数の第 4 引数として渡します (以下を参照) 。参照ベクトルには、その数と正確に同じ数の要素が必要であり、指定する値は `stride` の倍数でなければなりません。

<div id="vector-search-functions">
  ## ベクトル検索関数
</div>

以下は、`QBit` データ型を使用するベクトル類似度検索用の距離関数です。

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

ストライド付き `QBit` の場合、これらの関数は省略可能な第4引数 `used_dims` (先頭から読み取る次元数) を受け付けます。この引数を指定すると、それらの次元をカバーするストライドグループのみを読み取ります。

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```