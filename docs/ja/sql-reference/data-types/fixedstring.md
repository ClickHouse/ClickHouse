---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: FixedString(N)
---

# Fixedstring {#fixedstring}

の固定長文字列 `N` バイト(文字もコードポイントもない)。

の列を宣言するには `FixedString` タイプ、次の構文を使用します:

``` sql
<column_name> FixedString(N)
```

どこに `N` 自然数です。

その `FixedString` 型は、データの長さが正確である場合に効率的です `N` バイト。 他のすべてのケースでは、効率を低下させる可能性があります。

効率的に格納できる値の例 `FixedString`-型指定された列:

-   IPアドレスのバイナリ表現 (`FixedString(16)` IPv6用）。
-   Language codes (ru\_RU, en\_US … ).
-   Currency codes (USD, RUB … ).
-   ハッシュのバイナリ表現 (`FixedString(16)` MD5の場合, `FixedString(32)` 用SHA256)。

UUID値を格納するには、以下を使用します [UUID](uuid.md) データ型。

データを挿入するときは、clickhouse:

-   文字列が含まれていない場合、nullバイトの文字列を補完します `N` バイト。
-   スロー `Too large value for FixedString(N)` る場合の例外の文字列です。 `N` バイトまでとなります。

データを選択するとき、clickhouseは文字列の末尾にあるnullバイトを削除しません。 を使用する場合 `WHERE` この節では、nullバイトを手動で追加して、 `FixedString` 値。 次の例では、次の例を使用する方法を示します `WHERE` との節 `FixedString`.

のは、単一の次の表を考えてみましょう `FixedString(2)` 列:

``` text
┌─name──┐
│ b     │
└───────┘
```

クエリ `SELECT * FROM FixedStringTable WHERE a = 'b'` 結果としてデータを返さない。 このフィルターパターンはnullバイトまでとなります。

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

この動作は、mysqlとは異なります `CHAR` タイプ（文字列は空白で埋められ、空白は出力用に削除されます）。

の長さに注意してください。 `FixedString(N)` 値が一定になります。 その [長さ](../../sql-reference/functions/array-functions.md#array_functions-length) 関数の戻り値 `N` 場合においても `FixedString(N)` 値はnullバイトでのみ入力されるが、 [空](../../sql-reference/functions/string-functions.md#empty) 関数の戻り値 `1` この場合。

[元の記事](https://clickhouse.tech/docs/en/data_types/fixedstring/) <!--hide-->
