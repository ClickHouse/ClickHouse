---
description: '名前付き定数値の集合を表す ClickHouse の Enum データ型に関するドキュメント'
sidebar_label: 'Enum'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: 'Enum'
doc_type: 'reference'
---

名前付きの値で構成される列挙型です。

名前付きの値は、`'string' = integer` の組、または `'string'` の名前として宣言できます。ClickHouse に保存されるのは数値だけですが、値は名前を使って操作できます。

ClickHouse では、次をサポートしています。

* 8 ビットの `Enum`。`[-128, 127]` の範囲で列挙された最大 256 個の値を含めることができます。
* 16 ビットの `Enum`。`[-32768, 32767]` の範囲で列挙された最大 65536 個の値を含めることができます。

ClickHouse は、データの挿入時に `Enum` の型を自動的に選択します。保存サイズを明示的に指定したい場合は、`Enum8` または `Enum16` 型を使用することもできます。

<div id="usage-examples">
  ## 使用例
</div>

ここでは、`Enum8('hello' = 1, 'world' = 2)` 型のカラムを持つテーブルを作成します。

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

同様に、番号を省略することもできます。ClickHouse が連番を自動的に割り当てます。デフォルトでは、1 から順に割り当てられます。

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

最初の名前に使用する有効な開始番号を指定することもできます。

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

カラム `x` には、型定義に列挙されている `'hello'` または `'world'` のいずれかの値しか格納できません。これ以外の値を格納しようとすると、ClickHouse で例外が発生します。この `Enum` には 8 ビット幅が自動的に選択されます。

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

テーブルのデータをクエリすると、ClickHouse は `Enum` の文字列値を出力します。

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

行に対応する数値を確認するには、`Enum` の値を整数型にキャストする必要があります。

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

クエリ内でEnumの値を作成するには、`CAST`も使用する必要があります。

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## 一般的なルールと使用方法
</div>

各値には、`Enum8` の場合は `-128 ... 127`、`Enum16` の場合は `-32768 ... 32767` の範囲内の数値が割り当てられます。文字列と数値は、すべて互いに異なっていなければなりません。空文字列も使用できます。この型が指定されている場合 (table definition 内) 、数値は任意の順序で指定できます。ただし、順序自体には意味がありません。

`Enum` の文字列値と数値は、どちらも [NULL](../../sql-reference/syntax.md) にできません。

`Enum` は [Nullable](../../sql-reference/data-types/nullable.md) 型に含めることもできます。したがって、次のクエリを使ってテーブルを作成する場合

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

これは`'hello'`や`'world'`だけでなく、`NULL`も格納できます。

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

RAM 内では、`Enum` カラムは、対応する数値の `Int8` または `Int16` と同じ方法で格納されます。

テキスト形式で読み取る際、ClickHouse は値を文字列として解析し、Enum の値の集合から対応する文字列を検索します。見つからない場合は例外が発生します。テキストフォーマットで読み取る場合は、文字列が読み取られ、対応する数値が参照されます。見つからない場合は例外が発生します。
テキスト形式で書き込む際は、値は対応する文字列として書き込まれます。カラムデータに不正なデータ (有効な集合に含まれない数値) が含まれている場合は、例外が発生します。バイナリ形式での読み取りと書き込みでは、`Int8` および `Int16` の data types と同じように動作します。
暗黙的なデフォルト値は、最も小さい数値に対応する値です。

`ORDER BY`、`GROUP BY`、`IN`、`DISTINCT` などでは、Enum は対応する数値と同じように振る舞います。たとえば、ORDER BY では数値順にソートされます。等価演算子および比較演算子も、Enum に対しては基になる数値に対する場合と同じように機能します。

Enum の値は数値と比較できません。Enum は定数文字列と比較できます。比較対象の文字列がその Enum の有効な値でない場合は、例外が発生します。IN Operator は、左辺に Enum、右辺に文字列の集合を置く形でサポートされています。これらの文字列は、対応する Enum の値です。

ほとんどの数値演算や文字列演算は、Enum の値に対して定義されていません。たとえば、Enum に数値を加算したり、Enum に文字列を連結したりすることはできません。
ただし、Enum には、その文字列値を返す組み込みの `toString` 関数があります。

Enum の値は、T を数値型とする `toT` 関数を使って数値型に変換することもできます。T が enum の基になる数値型に対応している場合、この変換のコストはゼロです。
値の集合だけを変更するのであれば、Enum type は ALTER を使ってコストなしで変更できます。ALTER を使うことで、Enum のメンバーは追加も削除も可能です (削除は、その値がテーブル内で一度も使われていない場合にのみ安全です) 。保護措置として、以前に定義された Enum メンバーの数値を変更すると例外が発生します。

ALTER を使用すると、Int8 を Int16 に変更する場合と同様に、Enum8 を Enum16 に、またはその逆に変更できます。

<div id="add-enum-values">
  ## ENUM値を追加
</div>

enum に新しい値を追加するための糖衣構文として、ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values) を使用できます

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```