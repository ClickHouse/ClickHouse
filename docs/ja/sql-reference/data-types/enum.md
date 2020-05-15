---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 50
toc_title: "\u5217\u6319\u578B"
---

# 列挙型 {#enum}

名前付きの値で構成される列挙型。

名前の値として宣言された `'string' = integer` ペア。 ClickHouseは数字のみを格納しますが、名前による値の操作をサポートします。

ClickHouse支援:

-   8ビット `Enum`. それはで列挙可能な256までの値を含むことができます `[-128, 127]` 範囲。
-   16ビット `Enum`. それはで列挙可能な65536まで値を含むことができます `[-32768, 32767]` 範囲。

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

列 `x` 型定義にリストされている値のみを格納できます: `'hello'` または `'world'`. 他の値を保存しようとすると、ClickHouseは例外を発生させます。 このため8ビットサイズ `Enum` 自動的に選択されます。

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

テーブルからデータをクエリすると、clickhouseから文字列の値が出力されます `Enum`.

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

行に相当する数値を表示する必要がある場合は、次の行をキャストする必要があります `Enum` 整数型への値。

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

クエリで列挙値を作成するには、次のものも使用する必要があります `CAST`.

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## 一般的なルールと使用法 {#general-rules-and-usage}

各値には、範囲内の数値が割り当てられます `-128 ... 127` のために `Enum8` または範囲で `-32768 ... 32767` のために `Enum16`. すべての文字列と数字は異なる必要があります。 空の文字列が許可されます。 この型が(テーブル定義で)指定されている場合、数値は任意の順序で指定できます。 しかし、順序は重要ではありません。

文字列と数値のどちらも `Enum` できる。 [NULL](../../sql-reference/syntax.md).

アン `Enum` に含まれることができる [Nullable](nullable.md) タイプ。 そのため、クエリを使用してテーブルを作成する場合

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

それだけでなく、 `'hello'` と `'world'`、しかし `NULL` 同様に、。

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

ラムでは、 `Enum` 列は次のように保存されます `Int8` または `Int16` 対応する数値の。

テキストフォームで読み取る場合、clickhouseは値を文字列として解析し、一連のenum値から対応する文字列を検索します。 見つからない場合は、例外がスローされます。 テキスト形式で読み込むと、文字列が読み込まれ、対応する数値が検索されます。 見つからない場合は、例外がスローされます。
テキスト形式で書くときは、値を対応する文字列として書き込みます。 列データにガベージ(有効なセットに含まれていない数値)が含まれている場合は、例外がスローされます。 バイナリ形式で読み書きするときは、int8とint16のデータ型と同じように動作します。
暗黙的なデフォルト値は、数値が最も小さい値です。

の間 `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` そして、Enumは対応する数字と同じように動作します。 たとえば、ORDER BYは数値的に並べ替えます。 等価演算子と比較演算子は、基になる数値と同じようにEnumでも機能します。

Enum値を数値と比較することはできません。 列挙型は、定数文字列と比較できます。 比較された文字列が列挙型の有効な値でない場合は、例外がスローされます。 IN演算子は、左側の列挙型と右側の文字列のセットでサポートされています。 文字列は、対応する列挙型の値です。

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
しかし、列挙型は自然を持っています `toString` 文字列値を返す関数。

また、enumの値は、以下を使用して数値型に変換できます。 `toT` ここで、Tは数値型です。 Tが列挙型の基になる数値型に対応する場合、この変換はゼロコストになります。
値のセットのみが変更された場合、列挙型は、alterを使用してコストをかけずに変更できます。 alterを使用して列挙型のメンバーを追加および削除することができます（削除された値がテーブルで一度も使用されていない場合にのみ、削除は安全で セーフガードとして、以前に定義されたenumメンバの数値を変更すると例外がスローされます。

ALTERを使用すると、Int8をInt16に変更するのと同じように、Enum8をEnum16に変更することも、その逆も可能です。

[元の記事](https://clickhouse.tech/docs/en/data_types/enum/) <!--hide-->
