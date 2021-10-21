---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u56FA\u5B9A\u6587\u5B57\u5217(N)"
---

# Fixedstring {#fixedstring}

固定長の文字列 `N` バイト(文字もコードポイントもない)。

の列を宣言するには `FixedString` 次の構文を使用します:

``` sql
<column_name> FixedString(N)
```

どこに `N` は自然数である。

その `FixedString` データが正確に長さを持つ場合、型は効率的です `N` バイト数。 他のすべてのケースでは、効率を低下させる可能性があります。

効率的に格納できる値の例 `FixedString`-型指定された列:

-   IPアドレスのバイナリ表現 (`FixedString(16)` IPv6の場合）。
-   Language codes (ru_RU, en_US … ).
-   Currency codes (USD, RUB … ).
-   ハッシュのバイナリ表現 (`FixedString(16)` MD5の場合, `FixedString(32)` SHA256）のため。

UUID値を格納するには、 [UUID](uuid.md) データ型。

データを挿入するとき、ClickHouse:

-   文字列が含まれている数がnullバイトの文字列を補完します `N` バイト数。
-   スロー `Too large value for FixedString(N)` 文字列に以下のものが含まれている場合は例外 `N` バイト数。

データを選択すると、ClickHouseは文字列の末尾にあるnullバイトを削除しません。 を使用する場合 `WHERE` この節では、nullバイトを手動で追加する必要があります。 `FixedString` 値。 次の例では、次の操作を実行する方法を示します `WHERE` との句 `FixedString`.

次の表を単一のもので考えてみましょう `FixedString(2)` 列:

``` text
┌─name──┐
│ b     │
└───────┘
```

クエリ `SELECT * FROM FixedStringTable WHERE a = 'b'` 結果としてデータを返しません。 このフィルターパターンはnullバイトまでとなります。

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

この動作はMySQLとは異なります。 `CHAR` type(文字列はスペースで埋められ、スペースは出力のために削除されます)。

の長さに注意してください。 `FixedString(N)` 値は一定です。 その [長さ](../../sql-reference/functions/array-functions.md#array_functions-length) 関数の戻り値 `N` たとえ `FixedString(N)` 値はnullバイトのみで埋められますが、 [空](../../sql-reference/functions/string-functions.md#empty) 関数の戻り値 `1` この場合。

[元の記事](https://clickhouse.com/docs/en/data_types/fixedstring/) <!--hide-->
