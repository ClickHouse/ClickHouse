---

slug: /ja/sql-reference/syntax
sidebar_position: 2
sidebar_label: 文法

---

# 文法

システムには二種類のパーサーがあります：完全なSQLパーサー（再帰下降パーサー）とデータフォーマットパーサー（高速ストリームパーサー）です。`INSERT`クエリを除くすべての場合で、完全なSQLパーサーのみが使用されます。`INSERT`クエリは両方のパーサーを使用します：

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

`INSERT INTO t VALUES`フラグメントは完全なパーサーによって解析され、データ`(1, 'Hello, world'), (2, 'abc'), (3, 'def')`は高速ストリームパーサーによって解析されます。[input_format_values_interpret_expressions](../operations/settings/settings-formats.md#input_format_values_interpret_expressions)設定を使用してデータに対して完全なパーサーをオンにすることもできます。`input_format_values_interpret_expressions = 1`の場合、ClickHouseは最初に高速ストリームパーサーで値を解析しようとします。失敗した場合、ClickHouseはデータをSQL [式](#expressions)のように扱い、完全なパーサーを使用して解析しようとします。

データは任意のフォーマットを持つことができます。クエリが受信されると、サーバーはRAM内でリクエストの[最大クエリサイズ](../operations/settings/settings.md#max_query_size)バイト（デフォルトでは1 MB）を超えないように計算し、残りはストリーム解析されます。これにより、大量の`INSERT`クエリによる問題を回避できます。

`INSERT`クエリで`Values`フォーマットを使用する場合、データが`SELECT`クエリの式と同じように解析されるように見えることがありますが、これは真実ではありません。`Values`フォーマットははるかに制限されています。

この記事の残りの部分では完全なパーサーを取り上げます。フォーマットパーサーの詳細については、[フォーマット](../interfaces/formats.md)セクションを参照してください。

## スペース

構文構造の間（クエリの開始と終了を含む）には、任意の数のスペース記号が入ることがあります。スペース記号には、スペース、タブ、改行、キャリッジリターン、およびフォームフィードが含まれます。

## コメント

ClickHouseはSQLスタイルおよびCスタイルのコメントをサポートしています：

- SQLスタイルのコメントは`--`、`#!`または`# `で始まり、行の最後まで続きます。`--`と`#!`の後のスペースは省略可能です。
- Cスタイルのコメントは`/*`から`*/`までで、複数行にわたることができ、スペースも必要ありません。

## キーワード

キーワードは以下の場合に大文字小文字を区別しません：

- SQL標準に該当する場合。たとえば、`SELECT`、`select`、および`SeLeCt`はすべて有効です。
- 一部の人気DBMS（MySQLまたはPostgres）での実装に該当する場合。たとえば、`DateTime`は`datetime`と同じです。

データ型名が大文字小文字を区別するかどうかは、[system.data_type_families](../operations/system-tables/data_type_families.md#system_tables-data_type_families)テーブルで確認できます。

標準SQLと対照的に、他のすべてのキーワード（関数名を含む）は**大文字小文字を区別**します。

キーワードは予約されていません；それらは対応するコンテキストでのみそのように扱われます。キーワードと同じ名前の[識別子](#identifiers)を使用する場合、それらを二重引用符またはバックティックで囲みます。たとえば、クエリ`SELECT "FROM" FROM table_name`は、`table_name`テーブルに`"FROM"`という名前のカラムがある場合に有効です。

## 識別子

識別子とは：

- クラスター、データベース、テーブル、パーティション、カラム名。
- 関数。
- データ型。
- [式のエイリアス](#expression-aliases)。

識別子はクォート付きまたは非クォート付きがありえます。後者が推奨されます。

非クォート付きの識別子は、正規表現`^[a-zA-Z_][0-9a-zA-Z_]*$`と一致し、[キーワード](#keywords)と等しくない必要があります。例：`x`、`_1`、`X_y__Z123_`。

キーワードと同じ識別子を使用したい場合や識別子に他の記号を使用したい場合、二重引用符またはバックティックを使用してクォートしてください。例： `"id"`, `` `id` ``。

## リテラル

リテラルには、数値リテラル、文字列リテラル、複合リテラル、`NULL`リテラルがあります。

### 数値

数値リテラルは以下のように解析されます：

- 最初に、[strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul)関数を用いて64ビット符号付き整数として解析されます。
- 失敗した場合、[strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol)関数を用いて64ビット符号なし整数として解析されます。
- これでも失敗した場合、[strtod](https://en.cppreference.com/w/cpp/string/byte/strtof)関数を用いて浮動小数点数として解析されます。
- それ以外の場合はエラーを返します。

リテラル値は、値が収まる最小の型にキャストされます。たとえば、1は`UInt8`として解析されますが、256は`UInt16`として解析されます。詳細については[データ型](../sql-reference/data-types/index.md)を参照してください。数値リテラル内のアンダースコア`_`は無視され、可読性を向上させるために使用できます。

サポートされる数値リテラルは以下の通りです：

**整数** – `1`, `10_000_000`, `18446744073709551615`, `01`  
**小数** – `0.1`  
**指数表記** - `1e100`, `-1e-100`  
**浮動小数点数** – `123.456`, `inf`, `nan`

**16進数** – `0xc0fe`  
**SQL標準互換16進文字列** – `x'c0fe'`

**2進数** – `0b1101`  
**SQL標準互換2進文字列** – `b'1101'`

解釈の誤りを避けるため、8進数リテラルはサポートされていません。

### 文字列

文字列リテラルはシングルクォートで囲む必要があり、ダブルクォートはサポートされていません。エスケープは、次のいずれかで動作します：

- シングルクォートを前置することで、シングルクォート文字`'`（およびこの文字のみ）が`''`としてエスケープされます。
- バックスラッシュを前置することで、次のエスケープシーケンスがサポートされます：`\\`, `\'`, `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`。リストされていない文字の前にバックスラッシュがある場合、バックスラッシュはその特別な意味を失い、文字通りに解釈されます。

文字列リテラルでは、少なくとも`'`と`\`をエスケープコード`\'`（または：`''`）と`\\`でエスケープする必要があります。

### 複合 

配列は角括弧を用いて作成されます：`[1, 2, 3]`。タプルは丸括弧を用いて作成されます：`(1, 'Hello, world!', 2)`。技術的にはこれらはリテラルではなく、それぞれ配列生成演算子とタプル生成演算子を使用した式です。配列は少なくとも1つの項目を含む必要があり、タプルは少なくとも2つの項目を持つ必要があります。タプルは`SELECT`クエリの`IN`句で出現する特別なケースです。クエリの結果にはタプルを含めることができますが、タプルはデータベースに保存できません（[Memory](../engines/table-engines/special/memory.md)エンジンを使用するテーブルを除く）。

### NULL

値が欠落していることを示します。

テーブルフィールドに`NULL`を保存するには、フィールドが[Nullable](../sql-reference/data-types/nullable.md)型である必要があります。

データフォーマット（入力または出力）に応じて、`NULL`には異なる表現がある場合があります。詳細については、[データフォーマット](../interfaces/formats.md#formats)のドキュメントを参照してください。

`NULL`の処理には多くのニュアンスがあります。たとえば、比較操作の引数の少なくとも1つが`NULL`の場合、この操作の結果も`NULL`です。乗算や加算など他の操作についても同様です。詳細については、各操作のドキュメントを参照してください。

クエリ内で、[IS NULL](../sql-reference/operators/index.md#is-null)および[IS NOT NULL](../sql-reference/operators/index.md#is-not-null)演算子および関連する関数`isNull`および`isNotNull`を使用して`NULL`を確認できます。

### ヒアドキュメント

[ヒアドキュメント](https://en.wikipedia.org/wiki/Here_document)は、元のフォーマットを保持しながら文字列（多くの場合複数行）を定義する方法です。ヒアドキュメントはカスタム文字列リテラルとして定義され、たとえば`$heredoc$`のように二つの`$`記号で囲まれます。二つのヒアドキュメントの間にある値はそのまま処理されます。

ヒアドキュメントを使用して、SQL、HTML、またはXMLコードなどのスニペットを埋め込むことができます。

**例**

クエリ：

```sql
SELECT $smth$SHOW CREATE VIEW my_view$smth$;
```

結果：

```text
┌─'SHOW CREATE VIEW my_view'─┐
│ SHOW CREATE VIEW my_view   │
└────────────────────────────┘
```

## クエリパラメータの定義と使用法

クエリパラメータを使用すると、具体的な識別子の代わりに抽象的なプレースホルダーを含む一般的なクエリを書くことができます。クエリパラメータ付きのクエリが実行されると、すべてのプレースホルダーが解決され、実際のクエリパラメータの値に置き換えられます。

クエリパラメータを定義する方法は二つあります：

- `SET param_<name>=<value>` コマンドを使用する。
- コマンドラインで`clickhouse-client`に`--param_<name>='<value>'`を引数として渡す。`<name>`はクエリパラメータの名前、`<value>`はその値です。

クエリ内でクエリパラメータを参照するには、`{<name>: <datatype>}`を使用します。ここで`<name>`はクエリパラメータの名前、`<datatype>`はそれが変換されるデータ型です。

たとえば、以下のSQLは異なるデータ型を持つパラメータ`a` 、`b` 、`c` 、`d`を定義しています：

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
```

結果：

```response
13	str	2022-08-04 18:30:53	{'10':[11,12],'13':[14,15]}
```

`clickhouse-client`を使用している場合、パラメータは`--param_name=value`として指定されます。たとえば、以下のパラメータは`message`という名前で、`String`として取得されます：

```bash
clickhouse-client --param_message='hello' --query="SELECT {message: String}"
```

結果：

```response
hello
```

クエリパラメータがデータベース、テーブル、関数、またはその他の識別子の名前を表す場合、その型には`Identifier`を使用します。たとえば、次のクエリは`uk_price_paid`という名前のテーブルから行を返します：

```sql
SET param_mytablename = "uk_price_paid";
SELECT * FROM {mytablename:Identifier};
```

:::note
クエリパラメータは任意のSQLクエリの任意の場所に使用できる一般的なテキスト置換ではありません。主に識別子やリテラルの代わりに`SELECT`ステートメント内で機能するよう設計されています。
:::

## 関数

関数呼び出しは、括弧内に引数のリスト（空でも可）がある識別子のように書かれます。標準SQLとは対照的に、括弧は空の引数リストの場合でも必要です。例：`now()`。通常の関数と集計関数（[集計関数](/docs/ja/sql-reference/aggregate-functions/index.md)セクションを参照）が存在します。一部の集計関数は括弧内に引数リストを2つ持つことができます。例：`quantile (0.9) (x)`。これらの集計関数は「パラメトリック関数」と呼ばれ、最初のリストの引数は「パラメータ」と呼ばれます。パラメータのない集計関数の構文は、通常の関数と同じです。

## 演算子

演算子は、優先順位と結合性を考慮して、クエリ解析中に対応する関数に変換されます。たとえば、式 `1 + 2 * 3 + 4` は `plus(plus(1, multiply(2, 3)), 4)` に変換されます。

## データ型とデータベーステーブルエンジン

`CREATE`クエリ内のデータ型とテーブルエンジンは、識別子または関数として記述されます。つまり、括弧内に引数リストを含む場合と含まない場合があります。詳細については、[データ型](/docs/ja/sql-reference/data-types/index.md)、[テーブルエンジン](/docs/ja/engines/table-engines/index.md)、および[CREATE](/docs/ja/sql-reference/statements/create/index.md)セクションを参照してください。

## 式のエイリアス

エイリアスは、クエリ内の式に対するユーザー定義の名前です。

``` sql
expr AS alias
```

- `AS` — エイリアスを定義するためのキーワードです。テーブル名や`SELECT`句のカラム名に対して`AS`キーワードを使用せずにエイリアスを定義できます。

    例：`SELECT table_name_alias.column_name FROM table_name table_name_alias`

    [CAST](./functions/type-conversion-functions.md#castx-t) 関数内では、`AS` キーワードは別の意味を持ちます。関数の説明を参照してください。

- `expr` — ClickHouseによってサポートされる任意の式。

    例：`SELECT column_name * 2 AS double FROM some_table`

- `alias` — `expr`のための名前。エイリアスは[識別子](#identifiers)の構文に準拠する必要があります。

    例：`SELECT "table t".column_name FROM table_name AS "table t"`

### 使用に関する注意

エイリアスはクエリまたはサブクエリに対してグローバルであり、クエリの任意の部分で任意の式にエイリアスを定義できます。たとえば、`SELECT (1 AS n) + 2, n`。

エイリアスはサブクエリでは表示されず、サブクエリ間でも表示されません。たとえば、クエリ`SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a`を実行する際、ClickHouseは`Unknown identifier: num`という例外を生成します。

エイリアスがサブクエリの`SELECT`句における結果カラムに定義されている場合、これらのカラムは外部クエリで表示されます。たとえば、`SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`。

カラム名やテーブル名と同じエイリアスを注意してください。次の例を考えてみましょう：

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

この例では、テーブル`t`をカラム`b`で宣言しました。その後、データを選択する際に`sum(b) AS b`エイリアスを定義しました。エイリアスはグローバルであるため、ClickHouseは式`argMax(a, b)`内のリテラル`b`を式`sum(b)`で置き換えました。この置換により例外が発生しました。このデフォルトの挙動は、[prefer_column_name_to_alias](../operations/settings/settings.md#prefer-column-name-to-alias)を`1`に設定することで変更できます。

## アスタリスク

`SELECT`クエリでは、アスタリスクが式を置き換えることができます。詳細については、[SELECT](/docs/ja/sql-reference/statements/select/index.md#asterisk)セクションを参照してください。

## 式

式は、関数、識別子、リテラル、演算子の適用、括弧内の式、サブクエリ、またはアスタリスクです。エイリアスを含むこともできます。式のリストはカンマで区切られた1つ以上の式です。関数と演算子は、引数として式を持つことができます。
