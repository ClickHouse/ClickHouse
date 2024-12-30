---
slug: /ja/sql-reference/data-types/fixedstring
sidebar_position: 10
sidebar_label: FixedString(N)
---

# FixedString(N)

`N`バイトの固定長の文字列（文字やコードポイントではありません）。

`FixedString`型のカラムを宣言するには、以下の構文を使用します。

``` sql
<column_name> FixedString(N)
```

ここで、`N`は自然数です。

`FixedString`型は、データが正確に`N`バイトの長さを持つ場合に効率的です。それ以外の場合、効率が低下する可能性があります。

`FixedString`型のカラムに効率的に保存できる値の例：

- IPアドレスのバイナリ表現（IPv6に対しては`FixedString(16)`）。
- 言語コード（例: ru_RU, en_US）。
- 通貨コード（例: USD, RUB）。
- ハッシュのバイナリ表現（MD5に対しては`FixedString(16)`、SHA256に対しては`FixedString(32)`）。

UUID値を保存するためには、[UUID](../../sql-reference/data-types/uuid.md)データ型を使用してください。

データを挿入する際、ClickHouseは以下の処理を行います：

- 文字列が`N`バイト未満の場合、ヌルバイトで補完します。
- 文字列が`N`バイトを超える場合、`Too large value for FixedString(N)`例外をスローします。

データを選択する際、ClickHouseは文字列の末尾にあるヌルバイトを削除しません。`WHERE`句を使用する場合、`FixedString`値に一致させるため、手動でヌルバイトを追加する必要があります。以下の例は、`FixedString`と`WHERE`句をどのように使用するかを示しています。

次の単一の`FixedString(2)`カラムを含むテーブルを考えてみましょう。

``` text
┌─name──┐
│ b     │
└───────┘
```

クエリ `SELECT * FROM FixedStringTable WHERE a = 'b'` はデータを返しません。フィルタパターンをヌルバイトで補完する必要があります。

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

この動作は、MySQLの`CHAR`型とは異なります（そこでは文字列はスペースでパディングされ、出力の際にはスペースは削除されます）。

`FixedString(N)`の値の長さは一定であることに注意してください。[length](../../sql-reference/functions/array-functions.md#array_functions-length)関数は、`FixedString(N)`の値がヌルバイトのみで埋められていても`N`を返しますが、[empty](../../sql-reference/functions/string-functions.md#empty)関数はこの場合に`1`を返します。
