---
description: 'FUNCTION のドキュメント'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - ユーザー定義関数 (UDF)'
doc_type: 'reference'
---

ラムダ式からユーザー定義関数 (UDF) を作成します。この式は、関数のパラメーター、定数、演算子、または他の関数呼び出しのみで構成されている必要があります。

**構文**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

関数には、任意の数のパラメーターを指定できます。

いくつかの制約があります。

* 関数名は、ユーザー定義関数およびシステム関数の中で一意である必要があります。
* 再帰関数は使用できません。
* 関数で使用するすべての変数は、そのパラメーター一覧に指定する必要があります。

いずれかの制約に違反すると、例外がスローされます。

**例**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

次のクエリでは、ユーザー定義関数内で[条件関数](../../../sql-reference/functions/conditional-functions.md)が呼び出されています:

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

既存のUDFを置き換える:

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## 関連コンテンツ
</div>

<div id="executable-udfs">
  ### [実行可能 UDF](/ja/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [ClickHouse Cloud のユーザー定義関数](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
