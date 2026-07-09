---
description: 'SELECT クエリのドキュメント'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'SELECT クエリ'
doc_type: 'reference'
---

`SELECT` クエリはデータを取得します。デフォルトでは、要求したデータがクライアントに返されますが、[INSERT INTO](../../../sql-reference/statements/insert-into.md) と組み合わせることで、別のテーブルに転送することもできます。

<div id="syntax">
  ## 構文
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

`SELECT` の直後に続く必須の式リストを除き、すべての句は省略可能です。このリストについては[以下](#select-clause)で詳しく説明します。

各省略可能な句の詳細は個別のセクションで説明しており、実行順に以下のとおり掲載しています。

* [WITH 句](../../../sql-reference/statements/select/with.md)
* [SELECT 句](#select-clause)
* [DISTINCT 句](../../../sql-reference/statements/select/distinct.md)
* [FROM 句](../../../sql-reference/statements/select/from.md)
* [SAMPLE 句](../../../sql-reference/statements/select/sample.md)
* [JOIN 句](../../../sql-reference/statements/select/join.md)
* [PREWHERE 句](../../../sql-reference/statements/select/prewhere.md)
* [WHERE 句](../../../sql-reference/statements/select/where.md)
* [WINDOW 句](../../../sql-reference/window-functions/index.md)
* [GROUP BY 句](/ja/sql-reference/statements/select/group-by)
* [LIMIT BY 句](../../../sql-reference/statements/select/limit-by.md)
* [HAVING 句](../../../sql-reference/statements/select/having.md)
* [QUALIFY 句](../../../sql-reference/statements/select/qualify.md)
* [LIMIT 句](../../../sql-reference/statements/select/limit.md)
* [OFFSET 句](../../../sql-reference/statements/select/offset.md)
* [UNION 句](../../../sql-reference/statements/select/union.md)
* [INTERSECT 句](../../../sql-reference/statements/select/intersect.md)
* [EXCEPT 句](../../../sql-reference/statements/select/except.md)
* [INTO OUTFILE 句](../../../sql-reference/statements/select/into-outfile.md)
* [FORMAT 句](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## SELECT 句
</div>

`SELECT` 句で指定された[式](/ja/sql-reference/syntax#expressions)は、上記で説明した句のすべての操作が完了した後に計算されます。これらの式は、結果の各行に個別に適用されるものとして機能します。`SELECT` 句の式に集約関数が含まれている場合、ClickHouse は [GROUP BY](/ja/sql-reference/statements/select/group-by) 集約の際に、集約関数とその引数として使用される式を処理します。

結果にすべてのカラムを含めるには、アスタリスク (`*`) 記号を使用します。たとえば、`SELECT * FROM ...` です。

<div id="dynamic-column-selection">
  ### 動的なカラム選択
</div>

動的なカラム選択 (COLUMNS 式 とも呼ばれます) を使うと、クエリ結果内の一部のカラムを [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) 正規表現に一致させて選択できます。

```sql
COLUMNS('regexp')
```

たとえば、次のテーブルについて考えてみましょう:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

次のクエリは、カラム名に `a` を含むすべてのカラムからデータを取得します。

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

選択されたカラムは、アルファベット順に返されるわけではありません。

1 つのクエリで複数の `COLUMNS` 式を使用し、それらに関数を適用できます。

たとえば:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

`COLUMNS` 式が返す各カラムは、それぞれ個別の引数として関数に渡されます。また、関数が対応していれば、ほかの引数を渡すこともできます。関数を使用する際は注意してください。渡した引数の数に関数が対応していない場合、ClickHouse は例外をスローします。

例えば:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

この例では、`COLUMNS('a')` は 2 つのカラム `aa` と `ab` を返します。`COLUMNS('c')` は `bc` カラムを返します。`+` 演算子は 3 つの引数には適用できないため、ClickHouse は対応するメッセージを含む例外をスローします。

`COLUMNS` 式に一致するカラムは、異なるデータ型を持つ場合があります。`COLUMNS` がどのカラムにも一致せず、かつ `SELECT` 内の唯一の式である場合、ClickHouse は例外をスローします。

<div id="select-columns-with-like-or-ilike">
  #### `LIKE` または `ILIKE` を使用してカラムを選択する
</div>

`*` の後に、大文字と小文字を区別する `LIKE` または区別しない `ILIKE` を使って名前をパターンに一致させることで、カラムを選択することもできます。

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

`LIKE` と `ILIKE` のパターンは、正規表現ではなく、`LIKE` の構文に従います。`%` 文字は任意の文字列に一致し、`_` 文字は任意の1文字に一致し、`\` は `%`、`_`、`\` をエスケープします。両者の唯一の違いは、`LIKE` はカラム名を大文字と小文字を区別して照合するのに対し、`ILIKE` は大文字と小文字を区別しないことです。例えば:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

このクエリは、`aa` や `ab` のような、`a` で始まる2文字の名前を持つカラムを選択します。

`* LIKE` と `* ILIKE` は、修飾付きアスタリスクやカラムトランスフォーマーもサポートしています。

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### アスタリスク
</div>

クエリの任意の部分で、式の代わりにアスタリスクを使用できます。クエリが解析されると、アスタリスクはテーブル内のすべてのカラムのリスト (`MATERIALIZED` および `ALIAS` カラムを除く) に展開されます。アスタリスクの使用が妥当なのは、次のような限られたケースだけです。

* テーブルのダンプを作成する場合。
* システムテーブルのように、カラム数がごく少ないテーブルの場合。
* テーブルにどのカラムがあるかを確認する場合。この場合は `LIMIT 1` を設定します。ただし、`DESC TABLE` クエリを使用するほうが適切です。
* `PREWHERE` を使って少数のカラムに対して強いフィルタリングを行う場合。
* サブクエリ内 (外部クエリで不要なカラムはサブクエリから除外されるため) 。

それ以外のすべてのケースでは、アスタリスクの使用は推奨しません。というのも、列指向 DBMS の利点ではなく、欠点だけをもたらすためです。言い換えると、アスタリスクの使用は推奨されません。

<div id="extreme-values">
  ### 極値
</div>

結果に加えて、結果カラムの最小値と最大値も取得できます。これを行うには、**extremes** 設定を 1 にします。最小値と最大値は、数値型、日付型、および日時型に対して計算されます。その他のカラムについては、デフォルト値が出力されます。

追加の 2 行、つまり最小値と最大値がそれぞれ計算されます。これら 2 行は、他の行とは別に、`XML`、`JSON*`、`TabSeparated*`、`CSV*`、`Vertical`、`Template`、`Pretty*` の [フォーマット](../../../interfaces/formats.md) で出力されます。その他のフォーマットでは出力されません。

`JSON*` および `XML` フォーマットでは、極値は別個の `extremes` フィールドに出力されます。`TabSeparated*`、`CSV*`、`Vertical` フォーマットでは、この行はメインの結果の後に、`totals` がある場合はその後に続けて出力されます。その前には空行が挿入されます (他のデータの後) 。`Pretty*` フォーマットでは、この行はメインの結果の後に、`totals` がある場合はその後に続く別のテーブルとして出力されます。`Template` フォーマットでは、極値は指定したテンプレートに従って出力されます。

極値は `LIMIT` の前、ただし `LIMIT BY` の後の行に対して計算されます。ただし、`LIMIT offset, size` を使用する場合、`offset` より前の行も `extremes` に含まれます。ストリームリクエストでは、結果に `LIMIT` を通過した少数の行が追加で含まれることもあります。

<div id="notes">
  ### 注記
</div>

クエリのどの部分でも、シノニム (`AS` による別名) を使用できます。

`GROUP BY`、`ORDER BY`、`LIMIT BY` 句では、位置引数を使用できます。これを有効にするには、[enable&#95;positional&#95;arguments](/ja/operations/settings/settings#enable_positional_arguments) 設定を有効にします。たとえば、`ORDER BY 1,2` とすると、テーブルの行は 1 番目のカラム、次に 2 番目のカラムでソートされます。

<div id="implementation-details">
  ## 実装の詳細
</div>

クエリで `DISTINCT`、`GROUP BY`、`ORDER BY` 句、および `IN` と `JOIN` のサブクエリを省略した場合、そのクエリは O(1) 量の RAM で完全にストリーム処理されます。そうでない場合、適切な制限を指定しないと、大量の RAM を消費する可能性があります。

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

詳細については、「設定」セクションを参照してください。外部ソート (一時テーブルをディスクに保存すること) や外部集約を使用できます。

<div id="select-modifiers">
  ## SELECT 修飾子
</div>

`SELECT` クエリでは、次の修飾子を使用できます。

| Modifier                           | Description                                                                                                                                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`APPLY`](./apply_modifier.md)     | クエリの外側のテーブル式が返すすべての行に対して、関数を呼び出せます。                                                                                                                                                                          |
| [`EXCEPT`](./except_modifier.md)   | 結果から除外する 1 つ以上のカラム名を指定します。一致するカラム名はすべて出力から除外されます。                                                                                                                                                            |
| [`REPLACE`](./replace_modifier.md) | 1 つ以上の [式の別名](/ja/sql-reference/syntax#expression-aliases) を指定します。各別名は、`SELECT *` ステートメント内のカラム名と一致している必要があります。出力カラムの一覧では、別名に一致するカラムが、その `REPLACE` 内の式に置き換えられます。この修飾子はカラム名やカラムの順序を変更しません。ただし、値とその型は変更される場合があります。 |

<div id="modifier-combinations">
  ### 修飾子の組み合わせ
</div>

各修飾子は個別に使用することも、組み合わせて使用することもできます。

**例:**

同じ修飾子を複数回使用する例。

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

1つのクエリで複数の修飾子を使用する。

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SELECT クエリ内の SETTINGS
</div>

必要な設定は、`SELECT` クエリ内で直接指定できます。設定値はこのクエリにのみ適用され、クエリの実行後はデフォルト値または以前の値に戻ります。

設定を行うその他の方法については、[こちら](/ja/operations/settings/overview)を参照してください。

ブール値の設定を true にする場合は、値の代入を省略した短縮構文を使用できます。設定名だけを指定すると、自動的に `1` (true) に設定されます。

**例**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```