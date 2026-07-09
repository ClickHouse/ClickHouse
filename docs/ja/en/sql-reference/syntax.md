---
description: '構文に関するドキュメント'
sidebar_label: '構文'
sidebar_position: 2
slug: /sql-reference/syntax
title: '構文'
doc_type: 'reference'
---

このセクションでは、ClickHouse の SQL 構文について見ていきます。
ClickHouse は SQL ベースの構文を採用していますが、多くの拡張機能と最適化が加えられています。

<div id="query-parsing">
  ## クエリのパース
</div>

ClickHouse には 2 種類のパーサーがあります。

* *完全な SQL パーサー* (再帰下降パーサー) 。
* *データフォーマットパーサー* (高速ストリームパーサー) 。

完全な SQL パーサーは、`INSERT` クエリを除くすべてのケースで使用されます。`INSERT` クエリでは、両方のパーサーが使われます。

以下のクエリを見てみましょう。

```sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

すでに述べたように、`INSERT` クエリでは 2 種類のパーサーが使われます。
`INSERT INTO t VALUES` の部分はフルパーサーで解析され、
データ `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` はデータフォーマットパーサー、つまり高速ストリームパーサーで解析されます。

<details>
  <summary>フルパーサーを有効にする</summary>

  [`input_format_values_interpret_expressions`](../operations/settings/settings-formats.md#input_format_values_interpret_expressions) 設定を使用すると、
  データに対してフルパーサーを有効にすることもできます。

  前述の設定が `1` に設定されている場合、
  ClickHouse はまず高速ストリームパーサーで値を解析しようとします。
  失敗した場合は、そのデータを SQL の [式](#expressions) として扱い、フルパーサーでの解析を試みます。
</details>

データには任意のフォーマットを使用できます。
クエリを受信すると、サーバーはリクエストのうち [max&#95;query&#95;size](../operations/settings/settings.md#max_query_size) バイトまでしか RAM 上で計算せず
(デフォルトでは 1 MB) 、残りはストリーム解析されます。
これは、大きな `INSERT` クエリに伴う問題を回避するためです。なお、ClickHouse にデータを挿入する際は、この方法が推奨されています。

`INSERT` クエリで [`Values`](/ja/interfaces/formats/Values) フォーマットを使用する場合、
データは `SELECT` クエリの式と同じように解析されるように見えるかもしれませんが、実際には異なります。
`Values` フォーマットで扱える内容は大幅に制限されています。

このセクションの残りでは、フルパーサーについて説明します。

:::note
フォーマットパーサーの詳細については、[Formats](../interfaces/formats.md) セクションを参照してください。
:::

<div id="spaces">
  ## 空白
</div>

* 構文要素の間には、任意の数の空白文字を置くことができます (クエリの先頭および末尾を含む) 。
* 空白文字には、スペース、タブ、ラインフィード、CR、フォームフィードが含まれます。

<div id="comments">
  ## コメント
</div>

ClickHouse は、SQL スタイルのコメントと C スタイルのコメントの両方をサポートしています。

* SQL スタイルのコメントは `--`、`#!`、または `# ` で始まり、行末まで続きます。`--` と `#!` の後のスペースは省略可能です。
* C スタイルのコメント:
  * `//` (または `/` が 3 個以上連続するもの) の後にテキストが続き、行末までコメントとして扱われます。`/` の後にスペースは不要です。
  * 複数行コメントでは、`/*` から `*/` まで記述できます。この場合もスペースは不要です。
  * C スタイルのコメントは入れ子にできます。

たとえば:

```sql
/*
 * Compute the number of days between two dates.
 * /* Returns NULL if either argument is NULL */
 */
SELECT
    dateDiff('day', toDate('2024-01-01'), toDate('2024-12-31')) AS days_in_year, -- 365
    dateDiff('day', toDate('2020-01-01'), today()) AS days_since  #! since 2020
    ///////////////////////////////////////////////////////////////////
    # TODO: add hour/minute variants
```

<div id="keywords">
  ## キーワード
</div>

ClickHouse のキーワードは、コンテキストに応じて *case-sensitive* または *case-insensitive* になります。

キーワードが **case-insensitive** になるのは、次に該当する場合です。

* SQL 標準のキーワード。たとえば、`SELECT`、`select`、`SeLeCt` はいずれも有効です。
* 一般的な DBMS (MySQL や Postgres) での実装に由来するもの。たとえば、`DateTime` と `datetime` は同じです。

:::note
データ型の型名が case-sensitive かどうかは、[system.data&#95;type&#95;families](/ja/operations/system-tables/data_type_families) テーブルで確認できます。
:::

一方、標準 SQL とは異なり、それ以外のキーワード (関数名を含む) はすべて **case-sensitive** です。

また、キーワードは予約語ではありません。
キーワードとして扱われるのは、対応するコンテキストにおいてのみです。
キーワードと同じ名前の [識別子](#identifiers) を使う場合は、ダブルクォートまたはバッククォートで囲んでください。

たとえば、テーブル `table_name` に `"FROM"` という名前のカラムがある場合、次のクエリは有効です。

```sql
SELECT "FROM" FROM table_name
```

<div id="identifiers">
  ## 識別子
</div>

識別子には、次のものが含まれます。

* クラスター、データベース、テーブル、パーティション、カラム名
* [関数](#functions)
* [データ型](../sql-reference/data-types/index.md)
* [式の別名](#expression-aliases)

識別子は引用符で囲んでも囲まなくても記述できますが、後者が推奨されます。

引用符なしの識別子は、正規表現 `^[a-zA-Z_][0-9a-zA-Z_]*$` に一致し、かつ [キーワード](#keywords) と同一であってはなりません。
有効な識別子と無効な識別子の例については、以下の表を参照してください。

| 有効な識別子                                         | 無効な識別子                                 |
| ---------------------------------------------- | -------------------------------------- |
| `xyz`, `_internal`, `Id_with_underscores_123_` | `1x`, `tom@gmail.com`, `äußerst_schön` |

キーワードと同じ名前の識別子を使用したい場合や、識別子内で他の記号を使いたい場合は、たとえば `"id"` や `` `id` `` のように、二重引用符またはバッククォートで囲んでください。

:::note
引用符付き識別子でのエスケープに適用されるルールは、文字列リテラルにも同様に適用されます。詳細は [String](#string) を参照してください。
:::

:::tip[カラム名でドットを使用しない]
ドットを含むカラム名、共通のドット付きプレフィックスを持つカラム、および `Array` 型のカラムは、`flatten_nested = 1` (デフォルト) の場合、それぞれフラット化された `Nested` 構造の一部として解釈されることがあります。これにより、insert 時に予期しない配列長の検証や、名前変更の制限が発生する可能性があります。

可能であれば、カラム名にドットは使用しないでください。
意図的に `Nested` のセマンティクスが必要な場合を除き、カラム名ではドットの代わりにアンダースコア (`_`) または別の区切り文字を使用してください。
:::

<div id="literals">
  ## リテラル
</div>

ClickHouse では、リテラルとはクエリ内に直接記述される値のことです。
つまり、クエリの実行中に変化しない固定値です。

リテラルには、次の種類があります。

* [String](#string)
* [数値](#numeric)
* [複合](#compound)
* [`NULL`](#null)
* [ヒアドキュメント](#heredoc) (カスタム文字列リテラル)

以下の各節で、これらをさらに詳しく見ていきます。

<div id="string">
  ### String
</div>

文字列リテラルはシングルクォートで囲む必要があります。ダブルクォートはサポートされていません。

エスケープは、次のいずれかの方法で行います。

* シングルクォート文字 `'` の前にシングルクォートを付ける方法。この文字 (`'` のみ) は `''` としてエスケープできます。
* 下の表に示すサポート対象のエスケープシーケンスの前にバックスラッシュを付ける方法。

:::note
バックスラッシュの後に以下に挙げた文字以外が続く場合、バックスラッシュは特別な意味を失い、つまりそのままの文字として解釈されます。
:::

| Supported Escape                       | Description                                     |
| -------------------------------------- | ----------------------------------------------- |
| `\xHH`                                 | 任意の数の16進数 (H) が続く 8 ビット文字指定。                    |
| `\N`                                   | 予約済みで、何もしません (例: `SELECT 'a\Nb'` は `ab` を返します)  |
| `\a`                                   | ベル                                              |
| `\b`                                   | バックスペース                                         |
| `\e`                                   | エスケープ文字                                         |
| `\f`                                   | フォームフィード                                        |
| `\n`                                   | ラインフィード                                         |
| `\r`                                   | 復帰                                              |
| `\t`                                   | 水平タブ                                            |
| `\v`                                   | 垂直タブ                                            |
| `\0`                                   | null 文字                                         |
| `\\`                                   | バックスラッシュ                                        |
| `\'` (or `''`)                         | シングルクォート                                        |
| `\"`                                   | ダブルクォート                                         |
| `` ` ``                                | バッククォート                                         |
| `\/`                                   | スラッシュ                                           |
| `\=`                                   | 等号                                              |
| ASCII control characters (c &lt;= 31). |                                                 |

:::note
文字列リテラルでは、少なくとも `'` と `\` は、エスケープコード `\'` (または `''`) および `\\` を使ってエスケープする必要があります。
:::

<div id="numeric">
  ### 数値
</div>

数値リテラルは、次のようにパースされます。

* リテラルの先頭にマイナス記号 `-` が付いている場合、そのトークンは一旦スキップされ、パース後に結果が符号反転されます。
* 数値リテラルは、まず [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) 関数を使って、64 ビット符号なし整数としてパースされます。
  * 値の先頭に `0b` または `0x`/`0X` が付いている場合、その数値はそれぞれ 2 進数または 16 進数としてパースされます。
  * 値が負で、その絶対値が 2<sup>63</sup> を超える場合は、エラーが返されます。
* これに失敗した場合、次にその値は [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) 関数を使って浮動小数点数としてパースされます。
* それ以外の場合は、エラーが返されます。

リテラル値は、その値が収まる最小の型に CAST されます。
例えば、次のとおりです。

* `1` は `UInt8` としてパースされます
* `256` は `UInt16` としてパースされます。

:::note Important
64 ビットを超える整数値 (`UInt128`、`Int128`、`UInt256`、`Int256`) は、正しくパースするには、より大きな型に CAST する必要があります。

```sql
-170141183460469231731687303715884105728::Int128
340282366920938463463374607431768211455::UInt128
-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256
115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256
```

これにより上記のアルゴリズムは使われず、任意精度をサポートするルーチンで整数がパースされます。

そうでない場合、リテラルは浮動小数点数としてパースされるため、切り捨てにより精度が失われる可能性があります。
:::

詳細については、[データ型](../sql-reference/data-types/index.md)を参照してください。

数値リテラル内のアンダースコア `_` は無視され、可読性を高めるために使用できます。

次の数値リテラルがサポートされています。

| 数値リテラル                | 例                                               |
| --------------------- | ----------------------------------------------- |
| **整数**                | `1`, `10_000_000`, `18446744073709551615`, `01` |
| **小数**                | `0.1`                                           |
| **指数表記**              | `1e100`, `-1e-100`                              |
| **浮動小数点数**            | `123.456`, `inf`, `nan`                         |
| **16進数**              | `0xc0fe`                                        |
| **SQL 標準互換の 16 進文字列** | `x'c0fe'`                                       |
| **2進数**               | `0b1101`                                        |
| **SQL 標準互換の 2 進文字列**  | `b'1101'`                                       |

:::note
解釈の誤りを防ぐため、8進数リテラルはサポートされていません。
:::

<div id="compound">
  ### 複合
</div>

Array は `[]` で作成します: `[1, 2, 3]`。Tuple は `()` で作成します: `(1, 'Hello, world!', 2)`。
厳密には、これらはリテラルではなく、それぞれ配列生成演算子およびタプル生成演算子を使った式です。
配列は少なくとも 1 つの要素で構成されている必要があり、タプルは少なくとも 2 つの要素を持っている必要があります。

:::note
Tuple が `SELECT` クエリの `IN` 句に現れる別のケースがあります。
クエリ結果には Tuple を含めることができますが、Tuple をデータベースに保存することはできません ([Memory](../engines/table-engines/special/memory.md) エンジンを使用するテーブルを除く) 。
:::

<div id="null">
  ### NULL
</div>

`NULL` は、値が存在しないことを示すために使用されます。
テーブルのフィールドに `NULL` を格納するには、そのフィールドが [Nullable](../sql-reference/data-types/nullable.md) 型である必要があります。

:::note
`NULL` については、次の点に注意してください。

* データのフォーマット (入力または出力) によっては、`NULL` の表現が異なる場合があります。詳しくは、[data formats](/ja/interfaces/formats) を参照してください。
* `NULL` の処理には注意が必要です。たとえば、比較演算の引数のうち少なくとも 1 つが `NULL` の場合、その演算結果も `NULL` になります。これは乗算、加算、その他の操作でも同様です。各操作のドキュメントを確認することをおすすめします。
* クエリでは、[`IS NULL`](/ja/sql-reference/functions/functions-for-nulls#isNull) および [`IS NOT NULL`](/ja/sql-reference/functions/functions-for-nulls#isNotNull) 演算子と、関連する関数 `isNull` および `isNotNull` を使用して `NULL` を確認できます。
  :::

<div id="heredoc">
  ### ヒアドキュメント
</div>

[ヒアドキュメント](https://en.wikipedia.org/wiki/Here_document)は、元のフォーマットを保ったまま文字列 (多くの場合は複数行) を定義するための方法です。
ヒアドキュメントは、2 つの`$`記号の間に記述するカスタム文字列リテラルとして定義されます。

例えば:

```sql
SELECT $heredoc$SHOW CREATE VIEW my_view$heredoc$;

┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

:::note

* 2 つのヒアドキュメントに挟まれた値は &quot;そのまま&quot; 処理されます。
  :::

:::tip

* ヒアドキュメントを使うと、SQL、HTML、XML などのコードスニペットを埋め込めます。
  :::

<div id="defining-and-using-query-parameters">
  ## クエリパラメータの定義と使用
</div>

クエリパラメータを使うと、具体的な識別子ではなく抽象的なプレースホルダーを含む汎用的なクエリを記述できます。
クエリパラメータを含むクエリが実行されると、
すべてのプレースホルダーが解決され、実際のクエリパラメータ値に置き換えられます。

クエリパラメータは、いくつかの方法で定義できます。

* `SET param_<name>=<value>` — クエリ内で `SET` コマンドを使用する方法。
* `--param_<name>='<value>'` — コマンドラインで `clickhouse-client` の引数として指定する方法。
* `param_<name>=<value>` — HTTP インターフェイスの URL クエリ文字列パラメータとして指定する方法。

クエリパラメータは、クエリ内で `{<name>: <datatype>}` を使って参照できます。ここで、`<name>` はクエリパラメータ名、`<datatype>` は変換先のデータ型です。

<details>
  <summary>SET コマンドを使った例</summary>

  たとえば、次の SQL では `a`、`b`、`c`、`d` という名前のパラメータを定義しており、それぞれ異なるデータ型を持ちます。

  ```sql
  SET param_a = 13;
  SET param_b = 'str';
  SET param_c = '2022-08-04 18:30:53';
  SET param_d = {'10': [11, 12], '13': [14, 15]};

  SELECT
     {a: UInt32},
     {b: String},
     {c: DateTime},
     {d: Map(String, Array(UInt8))};

  13    str    2022-08-04 18:30:53    {'10':[11,12],'13':[14,15]}
  ```
</details>

<details>
  <summary>clickhouse-client を使った例</summary>

  `clickhouse-client` を使用している場合、パラメータは `--param_name=value` として指定します。たとえば、次のパラメータは `message` という名前で、`String` として取得されます。

  ```bash
  clickhouse-client --param_message='hello' --query="SELECT {message: String}"

  hello
  ```

  クエリパラメータがデータベース、テーブル、関数、またはその他の識別子の名前を表す場合は、その型として `Identifier` を使用します。たとえば、次のクエリは `uk_price_paid` という名前のテーブルから行を返します。

  ```sql
  SET param_mytablename = "uk_price_paid";
  SELECT * FROM {mytablename:Identifier};
  ```
</details>

<details>
  <summary>HTTP インターフェイスを使った例</summary>

  クエリパラメータは、`param_` プレフィックス付きの URL クエリ文字列パラメータとして渡せます。たとえば、次のようになります。

  ```bash
  curl -s "http://localhost:8123/?param_message=hello" --data-binary "SELECT {message: String}"

  hello
  ```
</details>

<details>
  <summary>Web UI を使った例</summary>

  組み込みの Web UI (`play.html`) は、クエリ内の `{name:Type}` 形式のパラメータプレースホルダーを自動的に検出し、各パラメータに対応するラベル付き入力フィールドを表示します。パラメータ値は HTTP リクエストに含まれ、さらにブックマークや共有のためにページ URL にも保存されます。
</details>

:::note
クエリパラメータは、任意の SQL クエリの任意の場所で使える汎用的なテキスト置換ではありません。
主に、識別子やリテラルの代わりとして `SELECT` ステートメント内で使用することを想定して設計されています。
:::

<div id="functions">
  ## 関数
</div>

関数呼び出しは、識別子の後に `()` で囲んだ引数のリスト (空でも可) を付けて記述します。
標準 SQL とは異なり、引数リストが空の場合でも括弧は必須です。
例:

```sql
now()
```

次のものもあります。

* [通常の関数](/ja/sql-reference/functions/overview).
* [集約関数](/ja/sql-reference/aggregate-functions).

一部の集約関数では、括弧内に2つの引数リストを指定できます。たとえば:

```sql
quantile (0.9)(x) 
```

これらの集約関数は &quot;パラメトリック&quot; 関数と呼ばれ、
最初のリスト内の引数は &quot;パラメータ&quot; と呼ばれます。

:::note
パラメータを持たない集約関数の構文は、通常の関数と同じです。
:::

<div id="operators">
  ## 演算子
</div>

演算子は、クエリのパース時に、優先順位と結合性を考慮して対応する関数に変換されます。

たとえば、次の式

```text
1 + 2 * 3 + 4
```

へ変換されます

```text
plus(plus(1, multiply(2, 3)), 4)`
```

<div id="data-types-and-database-table-engines">
  ## データ型とデータベースのテーブルエンジン
</div>

`CREATE` クエリでは、データ型とテーブルエンジンは識別子や関数と同じように記述します。
つまり、括弧内の引数リストを含む場合もあれば、含まない場合もあります。

詳細については、次のセクションを参照してください。

* [データ型](/ja/sql-reference/data-types/index.md)
* [テーブルエンジン](/ja/engines/table-engines/index.md)
* [CREATE](/ja/sql-reference/statements/create/index.md).

<div id="expressions">
  ## 式
</div>

式には、次のいずれかを使用できます。

* 関数
* 識別子
* リテラル
* 演算子の適用
* 括弧で囲まれた式
* サブクエリ
* アスタリスク

また、[別名](#expression-aliases)を含めることもできます。

式のリストは、カンマで区切られた1つ以上の式で構成されます。
また、関数や演算子の引数には式を指定できます。

定数式とは、結果がクエリ分析の段階、つまり実行前に判明している式のことです。
たとえば、リテラルに対する式は定数式です。

<div id="expression-aliases">
  ## 式の別名
</div>

別名とは、クエリ内の[式](#expressions)に付けるユーザー定義の名前です。

```sql
expr AS alias
```

上記の構文の各部分について、以下で説明します。

| Part of syntax | Description                                                            | Example                                                                 | Notes                                                                                                        |
| -------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `AS`           | 別名を定義するためのキーワードです。`SELECT`句では、`AS`キーワードを使わなくても、テーブル名またはカラム名の別名を定義できます。 | `SELECT table_name_alias.column_name FROM table_name table_name_alias`. | [CAST](/ja/sql-reference/functions/type-conversion-functions#CAST) 関数では、`AS`キーワードは別の意味を持ちます。詳しくは関数の説明を参照してください。 |
| `expr`         | ClickHouse でサポートされている任意の式です。                                           | `SELECT column_name * 2 AS double FROM some_table`                      |                                                                                                              |
| `alias`        | `expr` に付ける名前です。別名は [識別子](#identifiers) の構文に従う必要があります。                 | `SELECT "table t".column_name FROM table_name AS "table t"`.            |                                                                                                              |

<div id="notes-on-usage">
  ### 使用上の注意
</div>

* 別名はクエリまたはサブクエリ全体で有効であり、任意の式に対する別名をクエリ内のどの部分でも定義できます。たとえば、次のとおりです。

```sql
SELECT (1 AS n) + 2, n`.
```

* 別名はサブクエリ内およびサブクエリ間では使用できません。たとえば、次のクエリを実行すると、ClickHouse は例外 `Unknown identifier: num` を返します。

```sql
`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`
```

* サブクエリの `SELECT` 句で結果カラムに別名が定義されている場合、それらのカラムは外側のクエリから参照できます。例えば:

```sql
SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.
```

* カラム名やテーブル名と同名の別名を使う場合は、注意してください。以下の例で見てみましょう。

```sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog();

SELECT
    argMax(a, b),
    sum(b) AS b
FROM t;

Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

前の例では、カラム `b` を持つテーブル `t` を宣言しました。
次に、データを選択する際に、`sum(b) AS b` という別名を定義しました。
別名はグローバルであるため、
ClickHouse は式 `argMax(a, b)` 内のリテラル `b` を式 `sum(b)` に置き換えました。
この置き換えによって例外が発生しました。

:::note
[prefer&#95;column&#95;name&#95;to&#95;alias](/ja/operations/settings/settings#prefer_column_name_to_alias) を `1` に設定すると、この既定の動作を変更できます。
:::

<div id="asterisk">
  ## アスタリスク
</div>

`SELECT` クエリでは、アスタリスクを式の代わりに使用できます。
詳細については、[SELECT](/ja/sql-reference/statements/select/index.md#asterisk) のセクションを参照してください。