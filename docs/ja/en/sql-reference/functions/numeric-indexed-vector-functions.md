---
description: 'NumericIndexedVector とその関数のドキュメント'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'NumericIndexedVector 関数'
doc_type: 'リファレンス'
---

NumericIndexedVector は、ベクトルをカプセル化し、ベクトルの集約操作と要素ごとの操作を実装する抽象データ構造です。Bit-Sliced Index はそのストレージ方式です。理論的な基礎とユースケースについては、論文 [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411) を参照してください。

<div id="bit-sliced-index">
  ## BSI
</div>

BSI (Bit-Sliced Index) ストレージ方式では、データは [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) 形式で格納され、その後 [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap) を使用して圧縮されます。集約操作や要素ごとの操作は圧縮データに対して直接実行されるため、ストレージ効率とクエリ効率を大幅に向上させることができます。

ベクトルには、インデックスとそれに対応する値が含まれます。以下は、BSI ストレージモードにおけるこのデータ構造の主な特性と制約です。

* 索引タイプには `UInt8`、`UInt16`、`UInt32` のいずれかを使用できます。**Note:** Roaring Bitmap の 64 ビット実装のパフォーマンスを考慮し、BSI フォーマットは `UInt64`/`Int64` をサポートしていません。
* 値の型には `Int8`、`Int16`、`Int32`、`Int64`、`UInt8`、`UInt16`、`UInt32`、`UInt64`、`Float32`、`Float64` のいずれかを使用できます。**Note:** 値の型は自動的には拡張されません。たとえば、値の型として `UInt8` を使用した場合、`UInt8` の上限を超える合計値はより上位の型に昇格されるのではなく、オーバーフローになります。同様に、整数に対する演算結果は整数になります (たとえば、除算しても自動的に浮動小数点の結果にはなりません) 。そのため、値の型は事前に十分検討して設計しておくことが重要です。実際の利用では、浮動小数点型 (`Float32`/`Float64`) がよく使われます。
* 演算を実行できるのは、索引タイプと値の型が同じ 2 つのベクトル同士のみです。
* 基盤となるストレージには Bit-Sliced Index が使われ、インデックスはビットマップに格納されます。ビットマップの具体的な実装には Roaring Bitmap が使用されます。圧縮率とクエリパフォーマンスを最大化するベストプラクティスは、インデックスをできるだけ少数の Roaring Bitmap コンテナーに集約することです。
* Bit-Sliced Index の仕組みでは、値をバイナリに変換します。浮動小数点型では、この変換に固定小数点表現が使われるため、精度が失われる可能性があります。精度は小数部に割り当てるビット数をカスタマイズすることで調整でき、デフォルトは 24 ビットです。これはほとんどのケースで十分です。NumericIndexedVector の構築時には、aggregate function groupNumericIndexedVector を `-State` とともに使用することで、整数部と小数部のビット数をカスタマイズできます。
* インデックスには、非ゼロ値、ゼロ値、存在しない値の 3 つのケースがあります。NumericIndexedVector には、非ゼロ値とゼロ値のみが格納されます。また、2 つの NumericIndexedVector 間で要素ごとの演算を行う場合、存在しないインデックスの値は 0 として扱われます。除算の場合、除数が 0 のとき結果は 0 になります。

<div id="create-numeric-indexed-vector-object">
  ## numericIndexedVector オブジェクトを作成する
</div>

この構造を作成する方法は 2 つあります。1 つは、集約関数 `groupNumericIndexedVector` を `-State` 付きで使用する方法です。
追加の条件を指定するには、接尾辞 `-if` を追加できます。
この集約関数は、その条件を満たす行のみを処理します。
もう 1 つは、`numericIndexedVectorBuild` を使用して map から構築する方法です。
`groupNumericIndexedVectorState` 関数では、パラメータを使って整数部と小数部のビット数をカスタマイズできますが、`numericIndexedVectorBuild` ではできません。

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

2 つのデータカラムから NumericIndexedVector を構築し、すべての値の合計を `Float64` 型で返します。接尾辞 `State` を付けると、NumericIndexedVector オブジェクトを返します。

**構文**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**パラメータ**

* `type`: String、任意。ストレージフォーマットを指定します。現在サポートされているのは `'BSI'` のみです。
* `integer_bit_num`: `UInt32`、任意。`'BSI'` ストレージフォーマットで有効です。このパラメータは整数部に使用するビット数を指定します。索引タイプが整数型の場合、デフォルト値はその索引の格納に使用されるビット数に対応します。たとえば、索引タイプが UInt16 の場合、デフォルトの `integer_bit_num` は 16 です。索引タイプが Float32 または Float64 の場合、integer&#95;bit&#95;num のデフォルト値は 40 であるため、表現可能なデータの整数部の範囲は `[-2^39, 2^39 - 1]` です。有効範囲は `[0, 64]` です。
* `fraction_bit_num`: `UInt32`、任意。`'BSI'` ストレージフォーマットで有効です。このパラメータは小数部に使用するビット数を指定します。値の型が整数の場合、デフォルト値は 0 です。値の型が Float32 または Float64 の場合、デフォルト値は 24 です。有効範囲は `[0, 24]` です。
* さらに、integer&#95;bit&#95;num + fraction&#95;bit&#95;num の有効範囲は [0, 64] でなければならないという制約もあります。
* `col1`: 索引カラム。サポートされる型: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`。
* `col2`: 値カラム。サポートされる型: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`。

**戻り値**

すべての値の合計を表す `Float64` 値。

**例**

テストデータ:

```text
UserID  PlayTime
1       10
2       20
3       30
```

クエリと結果：

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
以下のドキュメントは、`system.functions` システムテーブルから自動生成されています。
:::

{/* 
  以下のタグはシステムテーブルからドキュメントを生成するために使用されるため、削除しないでください。
  詳細については、https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md を参照してください。
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }