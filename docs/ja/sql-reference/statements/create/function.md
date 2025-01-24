---
slug: /ja/sql-reference/statements/create/function
sidebar_position: 38
sidebar_label: FUNCTION
title: "CREATE FUNCTION - ユーザー定義関数 (UDF)"
---

ラムダ式からユーザー定義関数 (UDF) を作成します。式は関数のパラメーター、定数、演算子、または他の関数呼び出しで構成する必要があります。

**構文**

```sql
CREATE FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```
関数は任意の数のパラメーターを持つことができます。

いくつかの制約事項があります：

- 関数の名前はユーザー定義関数およびシステム関数の中で一意でなければなりません。
- 再帰関数は許可されていません。
- 関数が使用するすべての変数は、そのパラメーターリストに指定されなければなりません。

いずれかの制約が違反された場合、例外が発生します。

**例**

クエリ:

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

結果:

``` text
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

以下のクエリでは、[条件関数](../../../sql-reference/functions/conditional-functions.md) がユーザー定義関数内で呼び出されます：

```sql
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

結果:

``` text
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

## 関連コンテンツ

### [実行可能なUDFs](/docs/ja/sql-reference/functions/udf.md).

### [ClickHouse Cloudのユーザー定義関数](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
