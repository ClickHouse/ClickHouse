---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "\u30A8\u30F3\u30B3\u30FC\u30C9"
---

# エンコード機能 {#encoding-functions}

## char {#char}

渡された引数の数として長さの文字列を返し、各バイトは対応する引数の値を持ちます。 数値型の複数の引数を受け取ります。 引数の値がUInt8データ型の範囲外である場合、丸めとオーバーフローの可能性があるUInt8に変換されます。

**構文**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**パラメータ**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../sql-reference/data-types/int-uint.md), [フロート](../../sql-reference/data-types/float.md).

**戻り値**

-   指定されたバイトの文字列。

タイプ: `String`.

**例**

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

を構築できます文字列の任意のエンコードに対応するバイトまでとなります。 UTF-8の例を次に示します:

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

## hex {#hex}

引数の十六進表現を含む文字列を返します。

**構文**

``` sql
hex(arg)
```

関数は大文字を使用しています `A-F` そして、接頭辞を使用しない（のような `0x`）または接尾辞（のような `h`).

整数引数の場合は、六桁を出力します (“nibbles”)最上位から最下位へ(ビッグエンディアンまたは “human readable” 順序）。 これは、最上位の非ゼロバイト（先頭のゼロバイトは省略されます）で始まりますが、先頭の数字がゼロであっても、常にすべてのバイトの両方の桁を

例:

**例**

クエリ:

``` sql
SELECT hex(1);
```

結果:

``` text
01
```

型の値 `Date` と `DateTime` 対応する整数(Dateの場合はエポックからの日数、DateTimeの場合はUnix Timestampの値)としてフォーマットされます。

のために `String` と `FixedString`、すべてのバイトは単に二進数として符号化される。 ゼロバイトは省略されません。

浮動小数点型とDecimal型の値は、メモリ内の表現としてエンコードされます。 支援においても少しエンディアン、建築、その符号化されたのでちょっとエンディアンです。※ 先頭/末尾のゼロバイトは省略されません。

**パラメータ**

-   `arg` — A value to convert to hexadecimal. Types: [文字列](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [フロート](../../sql-reference/data-types/float.md), [小数点](../../sql-reference/data-types/decimal.md), [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

**戻り値**

-   引数の十六進表現を持つ文字列。

タイプ: `String`.

**例**

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

## unhex(str) {#unhexstr}

任意の数の十六進数を含む文字列を受け取り、対応するバイトを含む文字列を返します。 大文字と小文字の両方をサポートしていますa-F進の桁数は偶数である必要はありません。 奇数の場合、最後の桁は00-0Fバイトの最下位半分として解釈されます。 引数文字列に十六進以外の桁数が含まれている場合、実装定義の結果が返されます(例外はスローされません)。
結果を数値に変換する場合は、 ‘reverse’ と ‘reinterpretAsType’ 機能。

## UUIDStringToNum(str) {#uuidstringtonumstr}

形式で36文字を含む文字列を受け入れます `123e4567-e89b-12d3-a456-426655440000` これをFixedString(16)内のバイトセットとして返す。

## UUIDNumToString(str) {#uuidnumtostringstr}

FixedString(16)値を受け取ります。 36文字を含む文字列をテキスト形式で返します。

## ビットマスクトリスト(num) {#bitmasktolistnum}

整数を受け取ります。 ソース番号を合計する二つの累乗のリストを含む文字列を返します。 これらは、テキスト形式のスペースなしで、昇順でコンマ区切りです。

## ビットマスクトアレイ(num) {#bitmasktoarraynum}

整数を受け取ります。 合計されたときにソース番号を合計する二つのべき乗のリストを含むUInt64数値の配列を返します。 配列内の数値は昇順です。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
