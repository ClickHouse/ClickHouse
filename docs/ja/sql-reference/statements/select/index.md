---
slug: /ja/sql-reference/statements/select/
sidebar_position: 32
sidebar_label: SELECT
---

# SELECT クエリ

`SELECT` クエリはデータの取得を行います。デフォルトでは要求されたデータはクライアントに返されますが、[INSERT INTO](../../../sql-reference/statements/insert-into.md)と併用することで、異なるテーブルに転送することも可能です。

## 構文

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
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
[INTO OUTFILE filename [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

すべての句はオプションですが、`SELECT`の直後に必要な式のリストは[下記](#select-clause)で詳しく説明されています。

各オプション句の詳細は、実行順に書かれている別のセクションで説明されています：

- [WITH句](../../../sql-reference/statements/select/with.md)
- [SELECT句](#select-clause)
- [DISTINCT句](../../../sql-reference/statements/select/distinct.md)
- [FROM句](../../../sql-reference/statements/select/from.md)
- [SAMPLE句](../../../sql-reference/statements/select/sample.md)
- [JOIN句](../../../sql-reference/statements/select/join.md)
- [PREWHERE句](../../../sql-reference/statements/select/prewhere.md)
- [WHERE句](../../../sql-reference/statements/select/where.md)
- [GROUP BY句](../../../sql-reference/statements/select/group-by.md)
- [LIMIT BY句](../../../sql-reference/statements/select/limit-by.md)
- [HAVING句](../../../sql-reference/statements/select/having.md)
- [QUALIFY句](../../../sql-reference/statements/select/qualify.md)
- [LIMIT句](../../../sql-reference/statements/select/limit.md)
- [OFFSET句](../../../sql-reference/statements/select/offset.md)
- [UNION句](../../../sql-reference/statements/select/union.md)
- [INTERSECT句](../../../sql-reference/statements/select/intersect.md)
- [EXCEPT句](../../../sql-reference/statements/select/except.md)
- [INTO OUTFILE句](../../../sql-reference/statements/select/into-outfile.md)
- [FORMAT句](../../../sql-reference/statements/select/format.md)

## SELECT句

[式](../../../sql-reference/syntax.md#syntax-expressions) は、上記で説明したすべての句の操作完了後に `SELECT` 句で計算されます。これらの式は、結果に含まれる別々の行に対して適用されるかのように機能します。`SELECT`句に含まれる式が集計関数を含んでいる場合、ClickHouseは[GROUP BY](../../../sql-reference/statements/select/group-by.md) 集計中にこれらの集計関数とその引数として使用される式を処理します。

すべてのカラムを結果に含めたい場合は、アスタリスク (`*`) 記号を使用します。例えば、`SELECT * FROM ...` とします。

### 動的カラム選択

動的カラム選択（COLUMNS式とも呼ばれます）は、結果のいくつかのカラムを[re2](https://en.wikipedia.org/wiki/RE2_(software))正規表現でマッチさせることができます。

``` sql
COLUMNS('regexp')
```

例えば、次のテーブルを考えてみます：

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下のクエリは、名前に `a` が含まれるすべてのカラムからデータを選択します。

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

選択されたカラムはアルファベット順には返されません。

クエリ内で複数の `COLUMNS` 式を使用し、関数をそれに適用することもできます。

例えば：

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

`COLUMNS` 式で返された各カラムは、別々の引数として関数に渡されます。また、関数がサポートしている場合は他の引数を関数に渡すこともできます。関数を使用する際は注意が必要です。関数が渡された引数の数をサポートしていない場合、ClickHouseは例外をスローします。

例えば：

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

この例では、`COLUMNS('a')` は2つのカラム `aa` と `ab` を返します。`COLUMNS('c')` は `bc` カラムを返します。`+` 演算子は3つの引数に適用できないため、ClickHouseは関連するメッセージとともに例外をスローします。

`COLUMNS` にマッチしたカラムは異なるデータ型を持つことがあります。`COLUMNS` が何のカラムにもマッチせず、`SELECT`の唯一の式である場合、ClickHouse は例外をスローします。

### アスタリスク

クエリのいかなる部分にもアスタリスクを式の代わりに置くことができます。クエリが解析されると、アスタリスクはすべてのテーブルカラムのリストに展開されます（`MATERIALIZED` および `ALIAS` カラムを除く）。アスタリスクの使用が正当化されるのはわずかなケースのみです：

- テーブルのダンプを作成する場合。
- 数少ないカラムしか持たないテーブル、たとえばシステムテーブルの場合。
- テーブルにどのカラムが含まれているかの情報を取得する場合。この場合、 `LIMIT 1` を設定してください。しかし、`DESC TABLE` クエリを使用する方が良いです。
- `PREWHERE` を用いて少数のカラムに強いフィルタをかける場合。
- サブクエリ内（外部クエリに必要でないカラムはサブクエリから除外されるため）。

これら以外のケースでは、アスタリスクの使用は推奨されません。列指向DBMSの利点を活かすのではなく、逆に不利をもたらすからです。つまり、アスタリスクの使用は推奨されません。

### Extreme Values

結果に加えて、結果カラムの最小値と最大値も取得できます。これを行うには、**extremes** 設定を1に設定します。最小値と最大値は数値型、日付、日時について計算されます。他のカラムについてはデフォルト値が出力されます。

追加の2行が計算されます。最小値と最大値のそれぞれです。これらの追加の2行は、他の行とは別に`XML`、`JSON*`、`TabSeparated*`、`CSV*`、`Vertical`、`Template` および `Pretty*` [format](../../../interfaces/formats.md)で出力されます。他のフォーマットでは出力されません。

`JSON*` および `XML` フォーマットでは、極値は別の ‘extremes’ フィールドで出力されます。`TabSeparated*`、`CSV*` および `Vertical` フォーマットでは、主要な結果の後、もし ‘totals’ が存在する場合はその後に行が空の行の後に出力されます。`Pretty*` フォーマットでは、行が主要な結果の後、もし `totals` が存在する場合はそれに続いて別のテーブルとして出力されます。`Template` フォーマットでは指定されたテンプレートに従って極値が出力されます。

極値は `LIMIT` の前に行に対して計算されますが、`LIMIT BY` の後に計算されます。しかし、`LIMIT offset, size` を使用している場合、`offset` 前の行も `extremes` に含まれます。ストリームリクエストでは、結果は `LIMIT` を通過した少数の行を含むこともあります。

### 注意事項

クエリのどの部分でも別名（`AS`エイリアス）を使用できます。

`GROUP BY`、`ORDER BY` および `LIMIT BY` 句は位置引数をサポートすることができます。これを有効にするには、[enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments)設定をオンにします。たとえば、`ORDER BY 1,2` はテーブルの最初の、次に2番目のカラムで行をソートします。

## 実装の詳細

クエリが `DISTINCT`、`GROUP BY` および `ORDER BY` 句と `IN` および `JOIN` サブクエリを省略した場合、クエリは完全にストリーム処理され、O(1) のRAM量を使用します。それ以外の場合、適切な制限が指定されていないとクエリは大量のRAMを消費する可能性があります：

- `max_memory_usage`
- `max_rows_to_group_by`
- `max_rows_to_sort`
- `max_rows_in_distinct`
- `max_bytes_in_distinct`
- `max_rows_in_set`
- `max_bytes_in_set`
- `max_rows_in_join`
- `max_bytes_in_join`
- `max_bytes_before_external_sort`
- `max_bytes_before_external_group_by`

詳細については、「設定」セクションを参照してください。外部ソート（ディスクへの一時テーブルの保存）および外部集計を使用することが可能です。

## SELECT修飾子

`SELECT` クエリで次の修飾子を使用できます。

### APPLY

クエリの外部テーブル式によって返される各行に対して関数を呼び出すことができます。

**構文:**

``` sql
SELECT <expr> APPLY( <func> ) FROM [db.]table_name
```

**例:**

``` sql
CREATE TABLE columns_transformers (i Int64, j Int16, k Int64) ENGINE = MergeTree ORDER by (i);
INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);
SELECT * APPLY(sum) FROM columns_transformers;
```

```
┌─sum(i)─┬─sum(j)─┬─sum(k)─┐
│    220 │     18 │    347 │
└────────┴────────┴────────┘
```

### EXCEPT

結果から除外する一つまたは複数のカラム名を指定します。すべての一致するカラム名が出力から省略されます。

**構文:**

``` sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

**例:**

``` sql
SELECT * EXCEPT (i) from columns_transformers;
```

```
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```

### REPLACE

[式のエイリアス](../../../sql-reference/syntax.md#syntax-expression_aliases)を一つまたは複数指定します。それぞれのエイリアスは `SELECT *` 文のカラム名と一致しなければなりません。出力カラムリストでは、エイリアスに一致するカラムがその `REPLACE` に表現された式で置き換えられます。

この修飾子はカラム名や順序を変更しません。ただし、値や値の型を変更することがあります。

**構文:**

``` sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**例:**

``` sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```

### 修飾子の組み合わせ

各修飾子を個別に使用することも、組み合わせて使用することもできます。

**例**:

同じ修飾子を複数回使用する。

``` sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) from columns_transformers;
```

```
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

単一のクエリで複数の修飾子を使用する。

``` sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

## SELECT クエリでの SETTINGS

必要な設定を `SELECT` クエリ内で指定できます。この設定値はこのクエリにのみ適用され、クエリの実行が終了するとデフォルト値または以前の値にリセットされます。

他の設定方法については[こちら](../../../operations/settings/index.md)を参照してください。

**例**

``` sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```

