---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "\u69CB\u6587"
---

# 構文 {#syntax}

システムのパーサーには、完全SQLパーサー(再帰的降下パーサー)とデータ形式パーサー(高速ストリームパーサー)の二つのタイプがあります。
を除くすべてのケースで `INSERT` 完全なSQLパーサーのみが使用されます。
その `INSERT` クエリの両方を使用のパーサ:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

その `INSERT INTO t VALUES` フラグメントは完全なパーサーによって解析され、データは `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` 高速ストリームパーサーによって解析されます。 また、データの完全なパーサーをオンにするには [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) 設定。 とき `input_format_values_interpret_expressions = 1`,ClickHouseはまず、高速ストリームパーサーで値を解析しようとします。 失敗した場合、ClickHouseはデータに対して完全なパーサーを使用し、SQLのように扱います [式](#syntax-expressions).

データの形式は任意です。 クエリが受信されると、サーバーは以下の値を計算しません [max_query_size](../operations/settings/settings.md#settings-max_query_size) 要求のバイトはRAM(デフォルトでは1MB)で、残りはストリーム解析されます。
これを回避する問題の大き `INSERT` クエリ。

を使用する場合 `Values` フォーマット `INSERT` これは、データがaの式と同じように解析されるように見えるかもしれません `SELECT` クエリが、これは真実ではありません。 その `Values` 形式ははるかに限られています。

この記事の残りの部分は完全なパーサーをカバーします。 フォーマットパーサーの詳細については、 [形式](../interfaces/formats.md) セクション

## スペース {#spaces}

構文構成（クエリの開始と終了を含む）の間には、任意の数のスペースシンボルがあります。 スペース記号には、スペース、タブ、改行、CR、およびフォームフィードがあります。

## コメント {#comments}

ClickHouse支援のいずれかのSQL型は、Cスタイルのコメント.
SQLスタイルのコメントで始まる `--` そして、行の終わり、後のスペースに進みます `--` 省略できる。
Cスタイルは `/*` に `*/`そして複数行にすることができ、スペースも必要ありません。

## キーワード {#syntax-keywords}

キーワードでは、大文字と小文字が区別されません:

-   SQL標準。 例えば, `SELECT`, `select` と `SeLeCt` すべて有効です。
-   いくつかの一般的なDBMS（MySQLまたはPostgres）での実装。 例えば, `DateTime` と同じです `datetime`.

データ型名で大文字と小文字が区別されるかどうかは、 `system.data_type_families` テーブル。

標準SQLとは対照的に、他のすべてのキーワード(関数名を含む)は次のとおりです **大文字と小文字を区別**.

キーワードは予約されていません。 を使用する場合 [識別子](#syntax-identifiers) キーワードと同じ名前で、二重引用符またはバッククォートで囲みます。 たとえば、次のクエリです `SELECT "FROM" FROM table_name` テーブルの場合は有効です `table_name` 名前を持つ列があります `"FROM"`.

## 識別子 {#syntax-identifiers}

識別子は:

-   クラスターデータベース、テーブル、パーティション、カラム名になってしまいます
-   機能。
-   データ型。
-   [式エイリアス](#syntax-expression_aliases).

識別子で引用することは非引用されます。 後者が好ましい。

非引用識別子に一致しなければならなregex `^[a-zA-Z_][0-9a-zA-Z_]*$` と等しくすることはできません [キーワード](#syntax-keywords). 例: `x, _1, X_y__Z123_.`

キーワードと同じ識別子を使用する場合や、識別子に他の記号を使用する場合は、二重引用符またはバッククォートを使用して引用符で囲みます。, `"id"`, `` `id` ``.

## リテラル {#literals}

数値、文字列、複合、および `NULL` リテラル。

### 数値 {#numeric}

数値リテラルが解析されようとします:

-   まず、64ビット符号付きの数値として、 [ストルトゥール](https://en.cppreference.com/w/cpp/string/byte/strtoul) 機能。
-   失敗した場合は、64ビット符号なしの数値として、 [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) 機能。
-   失敗した場合は、浮動小数点数として [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) 機能。
-   それ以外の場合は、エラーを返します。

リテラル値は、値が収まる最小の型を持ちます。
たとえば、1は次のように解析されます `UInt8` しかし、256は次のように解析されます `UInt16`. 詳細については、 [データ型](../sql-reference/data-types/index.md).

例: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### 文字列 {#syntax-string-literal}

単一引quotesの文字列リテラルのみがサポートされます。 囲まれた文字はバックスラッシュエスケープできます。 以下のエスケープシーケンスに対応する特殊な値: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. それ以外の場合は、エスケープシーケンスの形式で `\c`,ここで `c` は任意の文字であり、 `c`. つまり、シーケンスを使用できます `\'`と`\\`. この値は [文字列](../sql-reference/data-types/string.md) タイプ。

文字列リテラルでは、少なくとも `'` と `\`. 単一引quotesは、単一引quote、リテラルでエスケープできます `'It\'s'` と `'It''s'` 等しい。

### 化合物 {#compound}

配列は角括弧で構成されます `[1, 2, 3]`. Nuplesは丸括弧で構成されています `(1, 'Hello, world!', 2)`.
技術的には、これらはリテラルではなく、それぞれ配列作成演算子とタプル作成演算子を持つ式です。
配列は少なくとも一つの項目で構成され、組は少なくとも二つの項目を持つ必要があります。
タプルが表示される場合は別のケースがあります `IN` aの節 `SELECT` クエリ。 クエリ結果には組を含めることができますが、組をデータベースに保存することはできません [メモリ](../engines/table-engines/special/memory.md) エンジン）。

### NULL {#null-literal}

値が欠落していることを示します。

保存するために `NULL` テーブルフィールドでは、 [Null可能](../sql-reference/data-types/nullable.md) タイプ。

データ形式（入力または出力）に応じて), `NULL` 異なる表現を有していてもよい。 詳細については、以下の文書を参照してください [データ形式](../interfaces/formats.md#formats).

処理に多くのニュアンスがあります `NULL`. たとえば、比較演算の引数のうち少なくともいずれかが次のようになっている場合 `NULL` この操作の結果も `NULL`. 乗算、加算、およびその他の演算についても同様です。 詳細については、各操作のドキュメントを参照してください。

クエリでは、以下を確認できます `NULL` を使用して [IS NULL](operators/index.md#operator-is-null) と [IS NOT NULL](operators/index.md) 演算子と関連する関数 `isNull` と `isNotNull`.

## 関数 {#functions}

関数呼び出しは、引数のリスト（空の場合もあります）を丸括弧で囲んだ識別子のように書かれます。 標準のSQLとは対照的に、空の引数リストであっても角かっこが必要です。 例: `now()`.
正規関数と集計関数があります（セクションを参照 “Aggregate functions”). 一部の集計関数を含むことができ二つのリストの引数ットに固定して使用します。 例: `quantile (0.9) (x)`. これらの集計関数は “parametric” 関数と最初のリストの引数が呼び出されます “parameters”. パラメータのない集計関数の構文は、通常の関数と同じです。

## 演算子 {#operators}

演算子は、クエリの解析中に、優先順位と連想を考慮して、対応する関数に変換されます。
たとえば、次の式は `1 + 2 * 3 + 4` に変換されます `plus(plus(1, multiply(2, 3)), 4)`.

## データの種類とデータベースのテーブルエンジン {#data_types-and-database-table-engines}

のデータ型とテーブルエンジン `CREATE` クエリは、識別子または関数と同じ方法で記述されます。 つまり、かっこ内に引数リストを含むことも、含まないこともできます。 詳細については “Data types,” “Table engines,” と “CREATE”.

## 式エイリアス {#syntax-expression_aliases}

別名は、クエリ内の式のユーザー定義名です。

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` を使用せずに句 `AS` キーワード。

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. エイリアスは [識別子](#syntax-identifiers) 構文。

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### 使用上の注意 {#notes-on-usage}

エイリアスは、クエリまたはサブクエリのグローバルであり、任意の式のクエリの任意の部分でエイリアスを定義できます。 例えば, `SELECT (1 AS n) + 2, n`.

エイリアスは、サブクエリおよびサブクエリ間では表示されません。 たとえば、クエリの実行中に `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouseは例外を生成します `Unknown identifier: num`.

の結果列に対してエイリアスが定義されている場合 `SELECT` サブクエリの句は、これらの列は、外部クエリで表示されます。 例えば, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

列名またはテーブル名と同じ別名に注意してください。 次の例を考えてみましょう:

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

この例では、テーブルを宣言しました `t` 列付き `b`. 次に、データを選択するときに、 `sum(b) AS b` 別名だ としてエイリアスは、グローバルClickHouse置換されているリテラル `b` 式では `argMax(a, b)` 式を使って `sum(b)`. この置換により例外が発生しました。

## アスタリスク {#asterisk}

で `SELECT` クエリー、アスタリスクで置き換え異なるアイコンで表示されます。 詳細については “SELECT”.

## 式 {#syntax-expressions}

式は、関数、識別子、リテラル、演算子の適用、角かっこ内の式、サブクエリ、またはアスタリスクです。 別名を含めることもできます。
式のリストは、カンマで区切られた一つ以上の式です。
関数と演算子は、引数として式を持つことができます。

[元の記事](https://clickhouse.tech/docs/en/sql_reference/syntax/) <!--hide-->
