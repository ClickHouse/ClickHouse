---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 31
toc_title: "\u69CB\u6587"
---

# 構文 {#syntax}

システムには、完全なsqlパーサー(再帰的降下パーサー)とデータフォーマットパーサー(高速ストリームパーサー)の二種類のパーサーがあります。
を除くすべての場合において `INSERT` クエリでは、完全なSQLパーサーのみが使用されます。
その `INSERT` クエリの両方を使用のパーサ:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

その `INSERT INTO t VALUES` フラグメントは完全なパーサーとデータによって解析されます `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` 高速ストリームパーサーによって解析されます。 データの完全なパーサーをオンにするには、次のコマンドを使用します [input\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) 設定。 とき `input_format_values_interpret_expressions = 1`、ClickHouseは最初に高速ストリームパーサーで値を解析しようとします。 失敗した場合、ClickHouseはデータの完全なパーサーを使用し、SQLのように扱います [式](#syntax-expressions).

データには任意の形式を使用できます。 クエリが受信されると、サーバーは以下を計算します [max\_query\_size](../operations/settings/settings.md#settings-max_query_size) RAM内の要求のバイト(デフォルトでは1MB)、残りはストリーム解析されます。
これはシステムに大きいの問題がないことを意味します `INSERT` MySQLのようなクエリ。

を使用する場合 `Values` フォーマット `INSERT` クエリは、データがaの式と同じように解析されるように見えるかもしれません `SELECT` クエリが、これは真実ではありません。 その `Values` 形式は、はるかに限られています。

次に、完全なパーサーをカバーします。 情報の形式のパーサは、 [形式](../interfaces/formats.md) セクション。

## スペース {#spaces}

構文構成（クエリの開始と終了を含む）の間には、任意の数のスペースシンボルが存在する可能性があります。 スペースシンボルには、スペース、タブ、改行、cr、フォームフィードがあります。

## コメント {#comments}

SQL形式およびC形式のコメントがサポートされています。
SQLスタイルのコメント:from `--` ラインの終わりまで。 後のスペース `--` 省略可能です。
Cスタイルのコメント：from `/*` に `*/`. これらのコメントは複数行にできます。 ここでもスペースは必要ありません。

## キーワード {#syntax-keywords}

キーワードが対応する場合、大文字と小文字は区別されません:

-   SQL標準。 例えば, `SELECT`, `select` と `SeLeCt` すべて有効です。
-   いくつかの一般的なdbms（mysqlまたはpostgres）での実装。 例えば, `DateTime` は同じとして `datetime`.

データ型名が大文字小文字を区別するかどうかをチェックできます。 `system.data_type_families` テーブル。

標準sqlとは対照的に、他のすべてのキーワード（関数名を含む）は **大文字と小文字を区別する**.

キーワードはこの数は予約されていません(そうとして構文解析キーワードに対応するコンテキスト. 使用する場合 [識別子](#syntax-identifiers) キーワードと同じで、引用符で囲みます。 たとえば、クエリ `SELECT "FROM" FROM table_name` テーブルの場合は有効です。 `table_name` 名前の列があります `"FROM"`.

## 識別子 {#syntax-identifiers}

識別子は:

-   クラスターデータベース、テーブル、パーティションおよびカラム名になってしまいます
-   機能。
-   データ型。
-   [式の別名](#syntax-expression_aliases).

識別子は、引用符または非引用することができます。 非引用符付き識別子を使用することをお勧めします。

非引用識別子に一致しなければならなregex `^[a-zA-Z_][0-9a-zA-Z_]*$` とに等しくすることはできません [キーワード](#syntax-keywords). 例: `x, _1, X_y__Z123_.`

キーワードと同じ識別子を使用する場合、または識別子に他の記号を使用する場合は、二重引用符またはバッククォートを使用して引用符を引用します。, `"id"`, `` `id` ``.

## リテラル {#literals}

以下があります：数値、文字列、複合および `NULL` リテラル

### 数値 {#numeric}

数値リテラルは解析を試みます:

-   最初に64ビットの符号付き数値として、 [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) 機能。
-   失敗した場合、64ビット符号なしの数値として、 [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) 機能。
-   失敗した場合は、浮動小数点数として [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) 機能。
-   それ以外の場合は、エラーが返されます。

対応する値は、値が収まる最小の型を持ちます。
たとえば、1は次のように解析されます `UInt8` しかし、256は次のように解析されます `UInt16`. 詳細については、 [データ型](../sql-reference/data-types/index.md).

例: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### 文字列 {#syntax-string-literal}

単一引quotesの文字列リテラルのみがサポートされます。 囲まれた文字はバックスラッシュでエスケープできます。 以下のエスケープシーケンスに対応する特殊な値: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. 他のすべての場合において、エスケープシーケンスの形式 `\c`、どこ `c` は任意の文字です。 `c`. つまり、次のシーケンスを使用できます `\'`と`\\`. この値は、 [文字列](../sql-reference/data-types/string.md) タイプ。

文字列リテラルでエスケープする必要がある文字の最小セット: `'` と `\`. 単一引quoteは、単一引quoteでエスケープすることができます。 `'It\'s'` と `'It''s'` 等しい。

### 化合物 {#compound}

配列では構文がサポートされます: `[1, 2, 3]` とタプル: `(1, 'Hello, world!', 2)`..
実際には、これらはリテラルではなく、配列作成演算子とタプル作成演算子を持つ式です。
配列によって構成された少なくとも一つの項目には、タプル以上あることが必要です。石二鳥の優れものだ。
タプルに使用のための特別な目的があります `IN` aの句 `SELECT` クエリ。 タプルはクエリの結果として取得できますが、データベースに保存することはできません（ただし、 [メモリ](../engines/table-engines/special/memory.md) テーブル）。

### NULL {#null-literal}

値が欠落していることを示します。

を格納するために `NULL` テーブルフィールドでは、テーブルフィールド [Nullable](../sql-reference/data-types/nullable.md) タイプ。

データ形式（入力または出力）に応じて), `NULL` 異なる表現を持つことがあります。 詳細については、以下の文書を参照してください [データ形式](../interfaces/formats.md#formats).

処理には多くの微妙な違いがあります `NULL`. たとえば、比較操作の引数のうちの少なくとも一つが `NULL` この操作の結果も次のようになります `NULL`. 乗算、加算、およびその他の演算についても同様です。 詳細については、各操作のドキュメントを参照してください。

クエリでは、以下を確認できます `NULL` を使用して [IS NULL](operators.md#operator-is-null) と [IS NOT NULL](operators.md) 演算子と関連する関数 `isNull` と `isNotNull`.

## 機能 {#functions}

関数は、括弧内の引数のリスト（おそらく空）を持つ識別子のように書かれています。 標準sqlとは対照的に、空の引数リストであっても括弧が必要です。 例えば: `now()`.
通常の関数と集計関数があります(セクションを参照 “Aggregate functions”). 一部の集計関数を含むことができ二つのリストの引数ットに固定して使用します。 例えば: `quantile (0.9) (x)`. これらの集計関数が呼び出される “parametric” 関数と最初のリストの引数が呼び出されます “parameters”. パラメータを指定しない集計関数の構文は、通常の関数と同じです。

## 演算子 {#operators}

演算子は、優先度と結合性を考慮して、クエリの解析中に対応する関数に変換されます。
たとえば、次の式 `1 + 2 * 3 + 4` に変換される。 `plus(plus(1, multiply(2, 3)), 4)`.

## データ型とデータベ {#data_types-and-database-table-engines}

のデータ型とテーブルエンジン `CREATE` クエリは、識別子または関数と同じ方法で記述されます。 言い換えれば、それらは括弧内に引数リストを含んでいてもいなくてもよい。 詳細については、以下を参照してください “Data types,” “Table engines,” と “CREATE”.

## 式の別名 {#syntax-expression_aliases}

別名は、クエリ内の式のユーザー定義名です。

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` 使用しない句 `AS` キーワード。

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. エイリアスはに従うべきです [識別子](#syntax-identifiers) 構文。

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### 使用上の注意 {#notes-on-usage}

エイリアスは、クエリまたはサブクエリのグローバルであり、任意の式のクエリの任意の部分にエイリアスを定義できます。 例えば, `SELECT (1 AS n) + 2, n`.

エイリアスは、サブクエリやサブクエリ間では表示されません。 たとえば、クエリの実行中などです `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouseは例外を生成します `Unknown identifier: num`.

結果列に別名が定義されている場合 `SELECT` サブクエリの句は、これらの列は、外側のクエリで表示されます。 例えば, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

列名またはテーブル名と同じ別名には注意してください。 次の例を考えてみましょう:

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

この例では、テーブルを宣言しました `t` コラムを使って `b`. 次に、データを選択するときに、 `sum(b) AS b` エイリアス としてエイリアスは、グローバルClickHouse置換されているリテラル `b` 式の中で `argMax(a, b)` 式を使って `sum(b)`. この置換によって例外が発生しました。

## アスタリスク {#asterisk}

で `SELECT` クエリー、アスタリスクで置き換え異なるアイコンで表示されます。 詳細については、以下を参照してください “SELECT”.

## 式 {#syntax-expressions}

式は、関数、識別子、リテラル、演算子の適用、角かっこ内の式、サブクエリ、またはアスタリスクです。 別名を含めることもできます。
式のリストは、コンマで区切られた式です。
関数と演算子は、次に、引数として式を持つことができます。

[元の記事](https://clickhouse.tech/docs/en/query_language/syntax/) <!--hide-->
