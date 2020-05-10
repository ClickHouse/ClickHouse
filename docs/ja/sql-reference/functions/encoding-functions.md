---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 52
toc_title: "\u30A8\u30F3\u30B3\u30FC\u30C9"
---

# エンコード関数 {#encoding-functions}

## 文字 {#char}

渡された引数の数として長さを持つ文字列を返し、各バイトは対応する引数の値を持ちます。 数値型の複数の引数を受け取ります。 引数の値がuint8データ型の範囲外である場合は、丸めとオーバーフローが可能な状態でuint8に変換されます。

**構文**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**パラメータ**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../sql-reference/data-types/int-uint.md), [フロート](../../sql-reference/data-types/float.md).

**戻り値**

-   指定されたバイトの文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

結果:

``` text
┌─hello─┐
│ hello │
└───────┘
```

を構築できます文字列の任意のエンコードに対応するバイトまでとなります。 utf-8の例を次に示します:

クエリ:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

結果:

``` text
┌─hello──┐
│ привет │
└────────┘
```

クエリ:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

結果:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## 六角 {#hex}

引数の十六進表現を含む文字列を返します。

**構文**

``` sql
hex(arg)
```

この関数は大文字を使用しています `A-F` 接頭辞（など）を使用しないでください `0x` または接尾辞（のような `h`).

整数引数の場合は、六角数字を出力します (“nibbles”)最も重要なものから最も重要なもの(ビッグエンディアンまたは “human readable” 順序）。 最も重要な非ゼロバイト（先行ゼロバイトは省略されています）で始まりますが、先行桁がゼロであっても常に各バイトの両方の桁を出力します。

例えば:

**例えば**

クエリ:

``` sql
SELECT hex(1);
```

結果:

``` text
01
```

タイプの値 `Date` と `DateTime` 対応する整数としてフォーマットされます(日付のエポックからの日数と、DateTimeのUnixタイムスタンプの値)。

のために `String` と `FixedString` すべてのバイトは、単に二進数として符号化される。 ゼロバイトは省略されません。

浮動小数点型と小数型の値は、メモリ内での表現としてエンコードされます。 支援においても少しエンディアン、建築、その符号化されたのでちょっとエンディアンです。※ ゼロ先行/末尾のバイトは省略されません。

**パラメータ**

-   `arg` — A value to convert to hexadecimal. Types: [文字列](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [フロート](../../sql-reference/data-types/float.md), [小数](../../sql-reference/data-types/decimal.md), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   引数の十六進表現を持つ文字列。

タイプ: `String`.

**例えば**

クエリ:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

クエリ:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

結果:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex(str)) {#unhexstr}

任意の数の進数を含む文字列を受け取り、対応するバイトを含む文字列を返します。 十六進数の数字は偶数である必要はありません。 奇数の場合、最後の桁は、00-0fバイトの最下位半分として解釈されます。 引数stringに十六進数以外の桁数が含まれている場合、実装定義の結果が返されます(例外はスローされません)。
結果を数値に変換したい場合は、 ‘reverse’ と ‘reinterpretAsType’ 機能。

## UUIDStringToNum(str)) {#uuidstringtonumstr}

次の形式の36文字を含む文字列を受け取ります `123e4567-e89b-12d3-a456-426655440000`、およびFixedString(16)のバイトのセットとして返します。

## UUIDNumToString(str)) {#uuidnumtostringstr}

FixedString(16)値を受け取ります。 テキスト形式で36文字を含む文字列を返します。

## ビットマスクトリスト(num) {#bitmasktolistnum}

整数を受け入れます。 合計されたときにソース番号を合計する二つの累乗のリストを含む文字列を返します。 これらは、昇順で、テキスト形式のスペースなしでコンマ区切りです。

## ビットマスクアレール(num) {#bitmasktoarraynum}

整数を受け入れます。 合計されたときにソース番号を合計する二つの累乗のリストを含むuint64数の配列を返します。 配列内の数字は昇順です。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
