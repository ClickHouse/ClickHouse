---
slug: /ja/sql-reference/data-types/enum
sidebar_position: 20
sidebar_label: Enum
---

# Enum

名前付き値の集合から成る列挙型です。

名前付き値は `'string' = integer` ペアや `'string'` 名として宣言できます。ClickHouse は数値のみを格納しますが、その名前を通じて値を操作することが可能です。

ClickHouse は以下をサポートします:

- 8ビットの `Enum`。`[-128, 127]` 範囲で列挙された最大256の値を含むことができます。
- 16ビットの `Enum`。`[-32768, 32767]` 範囲で列挙された最大65536の値を含むことができます。

ClickHouse はデータが挿入されたときに `Enum` の型を自動的に選択します。また、ストレージのサイズを確実にするために `Enum8` または `Enum16` 型を使用することもできます。

## 使用例

ここでは、`Enum8('hello' = 1, 'world' = 2)` 型のカラムを持つテーブルを作成します:

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

同様に、番号を省略することもできます。ClickHouse は連続する番号を自動的に割り当てます。デフォルトでは 1 から始まります。

``` sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

最初の名前に対して許可される開始番号を指定することもできます。

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

``` sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

``` text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

カラム `x` には、型定義に記載されている `'hello'` または `'world'` のみを保存できます。その他の値を保存しようとすると、ClickHouse は例外を発生させます。この `Enum` のために 8ビットのサイズが自動的に選択されます。

``` sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

``` text
Ok.
```

``` sql
INSERT INTO t_enum values('a')
```

``` text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

テーブルからデータをクエリする場合、ClickHouse は `Enum` から文字列値を出力します。

``` sql
SELECT * FROM t_enum
```

``` text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

行の数値相当を確認する必要がある場合、`Enum` 値を整数型にキャストする必要があります。

``` sql
SELECT CAST(x, 'Int8') FROM t_enum
```

``` text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

クエリ内で Enum 値を作成するには、`CAST` を使用する必要があります。

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## 一般的なルールと使用法

各値には、`Enum8` の場合には `-128 ... 127`、`Enum16` の場合には `-32768 ... 32767` の範囲で番号が割り当てられます。すべての文字列と数値は異なる必要があります。空文字列も許可されています。この型が指定されている場合（テーブル定義内）、数値の順番は任意で構いません。しかし、順序は重要ではありません。

`Enum` で文字列または数値の値は [NULL](../../sql-reference/syntax.md) にすることはできません。

`Enum` は [Nullable](../../sql-reference/data-types/nullable.md) 型に含めることができます。したがって、次のクエリを使用してテーブルを作成すると、

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

`'hello'` と `'world'` のみならず、`NULL` も保存できるようになります。

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

RAM では、`Enum` カラムは対応する数値の `Int8` または `Int16` と同じ方法で保存されます。

テキスト形式で読み込むとき、ClickHouse は値を文字列として解析し、Enum 値の集合から対応する文字列を検索します。それが見つからない場合は例外が発生します。テキスト形式で書き込む際には、値を対応する文字列として出力します。カラムデータにごみ（有効な集合にない数値）が含まれている場合、例外が発生します。バイナリ形式での読み書きの際は、Int8 および Int16 のデータ型と同様の方法で動作します。暗黙のデフォルト値は最も低い番号の値です。

`ORDER BY`、`GROUP BY`、`IN`、`DISTINCT` などの際には、Enum は対応する数値と同じように動作します。例えば、ORDER BY は数値的にソートします。等価性および比較演算子は、Enum 上で基礎となる数値値と同じように機能します。

Enum 値は数値と比較できません。Enum は定数文字列と比較することができます。比較した文字列が Enum の有効な値でない場合、例外が発生します。Enum を左辺に、文字列の集合を右辺に持つ `IN` 演算子がサポートされています。文字列は対応する Enum の値です。

ほとんどの数値および文字列操作は Enum 値には定義されていません。例えば、Enum に数値を加算したり、Enum に文字列を連結したりすることはできません。しかし、Enum にはその文字列値を返す自然な `toString` 関数があります。

Enum 値は、`toT` 関数を使用して数値型に変換できます。ここで T は数値型です。T が enum の基礎となる数値型と一致する場合、この変換にコストはかかりません。
ALTER を使用して Enum 型を変更することはコストがかからずに行えますが、値の集合が変更される場合のみです。ALTER を使用して Enum のメンバーを追加および削除することが可能です（削除は削除する値がテーブルで一度も使用されていない場合のみ安全です）。安全策として、以前に定義された Enum メンバーの数値値を変更すると例外が発生します。

ALTER を使用することで Enum8 を Enum16 に、または Enum16 を Enum8 に変更することができます。これは Int8 を Int16 に変更するのと同様です。
