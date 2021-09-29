---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Enum
---

# Enum {#enum}

名前付き値で構成される列挙型。

名前の値として宣言された `'string' = integer` ペア。 ClickHouseは数値のみを格納しますが、名前による値の操作をサポートします。

ClickHouseサポート:

-   8ビット `Enum`. それはで列挙される256までの価値を含むことができます `[-128, 127]` 範囲
-   16ビット `Enum`. それはで列挙される65536までの価値を含むことができます `[-32768, 32767]` 範囲

ClickHouseは自動的に次のタイプを選択します `Enum` データが挿入されるとき。 また、 `Enum8` または `Enum16` ストレージのサイズを確認するタイプ。

## 使用例 {#usage-examples}

ここでは、 `Enum8('hello' = 1, 'world' = 2)` タイプ列:

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

列 `x` 型定義にリストされている値のみを格納できます: `'hello'` または `'world'`. 他の値を保存しようとすると、ClickHouseは例外を発生させます。 このための8ビットサイズ `Enum` 自動的に選択されます。

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

テーブルからデータを照会すると、ClickHouseは文字列値を `Enum`.

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

行の等価な数値を見る必要がある場合は、次のようにキャストする必要があります `Enum` 整数型の値。

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

クエリでEnum値を作成するには、次のものも使用する必要があります `CAST`.

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## 一般的なルールと使用法 {#general-rules-and-usage}

各値には、範囲内の数値が割り当てられます `-128 ... 127` のために `Enum8` または範囲 `-32768 ... 32767` のために `Enum16`. すべての文字列と数字は異なる必要があります。 空の文字列を使用できます。 この型が(テーブル定義で)指定されている場合、数値は任意の順序にすることができます。 しかし、順序は重要ではありません。

の文字列も数値もない。 `Enum` ことができます [NULL](../../sql-reference/syntax.md).

アン `Enum` に含めることができます [Null可能](nullable.md) タイプ。 だから、クエリを使用してテーブルを作成する場合

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

できるアプリ `'hello'` と `'world'` でも `NULL` 同様に。

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

RAMでは、 `Enum` 列は次のように格納されます `Int8` または `Int16` 対応する数値の。

テキスト形式で読み込む場合、ClickHouseは値を文字列として解析し、列挙型の値のセットから対応する文字列を検索します。 見つからない場合は、例外がスローされます。 テキスト形式で読み込むと、文字列が読み込まれ、対応する数値が検索されます。 見つからない場合は例外がスローされます。
テキスト形式で書き込む場合、その値を対応する文字列として書き込みます。 列データにガベージ(有効なセット以外の数値)が含まれている場合は、例外がスローされます。 バイナリ形式で読み書きする場合、Int8およびInt16データ型と同じように動作します。
暗黙的なデフォルト値は、数値が最も小さい値です。

中 `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` というように、列挙型は対応する数値と同じように動作します。 たとえば、ORDER BYは数値で並べ替えます。 等価演算子と比較演算子は、列挙型では、基になる数値と同じように動作します。

列挙型の値を数値と比較することはできません。 列挙型は定数文字列と比較できます。 比較対象の文字列が列挙型の有効な値でない場合は、例外がスローされます。 IN演算子は、左側の列挙型と右側の文字列のセットでサポートされています。 文字列は、対応する列挙型の値です。

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
しかし、Enumには自然なものがあります `toString` 文字列値を返す関数。

列挙型の値は、次の式を使用して数値型にも変換できます `toT` ここで、Tは数値型です。 Tが列挙型の基になる数値型に対応する場合、この変換はゼロコストになります。
列挙型は、値のセットのみが変更されている場合、ALTERを使用してコストなしで変更できます。 ALTERを使用して列挙型のメンバーを追加および削除することは可能です（削除された値がテーブルで使用されていない場合にのみ削除が安全です）。 セーフガードとして、以前に定義された列挙型メンバーの数値を変更すると、例外がスローされます。

ALTERを使用すると、Int8をInt16に変更するのと同じように、Enum8をEnum16に変更することも、その逆も可能です。

[元の記事](https://clickhouse.com/docs/en/data_types/enum/) <!--hide-->
