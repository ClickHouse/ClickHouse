---
slug: /ja/native-protocol/columns
sidebar_position: 4
---

# カラムタイプ

一般的な情報については、[データタイプ](https://clickhouse.com/docs/ja/sql-reference/data-types/)を参照してください。

## 数値タイプ

:::tip

数値タイプのエンコーディングは、AMD64やARM64のようなリトルエンディアンCPUのメモリレイアウトと一致しています。

これにより、非常に効率的なエンコーディングとデコーディングを実現できます。

:::

### 整数

IntとUIntの8, 16, 32, 64, 128または256ビットの文字列で、リトルエンディアン形式です。

### 浮動小数点数

IEEE 754のバイナリ表現でのFloat32とFloat64です。

## 文字列

単なる文字列の配列です。つまり、(len, value)。

## FixedString(N)

Nバイトシーケンスの配列です。

## IP

IPv4は`UInt32`数値型のエイリアスで、UInt32として表現されます。

IPv6は`FixedString(16)`のエイリアスで、バイナリで直接表現されます。

## Tuple

Tupleは単にカラムの配列です。例えば、Tuple(String, UInt8)は2つのカラムが連続してエンコードされたものです。

## Map

`Map(K, V)`は3つのカラムから成ります: `Offsets ColUInt64, Keys K, Values V`。

`Keys`と`Values`カラムの行数は`Offsets`の最後の値です。

## 配列

`Array(T)`は2つのカラムから成ります: `Offsets ColUInt64, Data T`。

`Data`の行数は`Offsets`の最後の値です。

## Nullable

`Nullable(T)`は`Nulls ColUInt8, Values T`という同じ行数を持つ内容で構成されています。

```go
// NullsはValuesカラムに対するnullableの「マスク」です。
// 例えば、[null, "", "hello", null, "world"]をエンコードする場合
//	Values: ["", "", "hello", "", "world"] (len: 5)
//	Nulls:  [ 1,  0,       0,  1,       0] (len: 5)
```

## UUID

`FixedString(16)`のエイリアスで、UUID値はバイナリで表現されます。

## Enum

`Int8`または`Int16`のエイリアスですが、それぞれの整数がいくつかの`String`値にマッピングされています。

## Low Cardinality

`LowCardinality(T)`は`Index T, Keys K`から成り、`K`は`Index`のサイズに応じて(UInt8, UInt16, UInt32, UInt64)のいずれかです。

```go
// インデックス(すなわち、Dictionary)カラムはユニークな値を含み、Keysカラムは
// インデックスカラムのインデックスのシーケンスを含み、それが実際の値を表します。
//
// 例えば、["Eko", "Eko", "Amadela", "Amadela", "Amadela", "Amadela"]は
// 以下のようにエンコードできます:
//	Index: ["Eko", "Amadela"] (String)
//	Keys:  [0, 0, 1, 1, 1, 1] (UInt8)
//
// CardinalityKeyはIndexサイズに応じて選択されます。選択されたタイプの最大値は、
// Index要素のインデックスを表現できる必要があります。
```

## Bool

`UInt8`のエイリアスであり、`0`はfalse、`1`はtrueを表します。
