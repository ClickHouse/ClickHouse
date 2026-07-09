---
description: '一時的な Merge テーブルを作成します。テーブルスキーマは、基になるテーブルのカラムのユニオンと共通の型の導出によって決まります。'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

一時的な [Merge](../../engines/table-engines/special/merge.md) テーブルを作成します。
テーブルスキーマは、基になるテーブルのカラムのユニオンと共通の型の導出によって決まります。
[Merge](../../engines/table-engines/special/merge.md) テーブルエンジンと同じ仮想カラムを利用できます。

<div id="syntax">
  ## 構文
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## 引数
</div>

| Argument        | Description                                                                                                                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | 設定可能な値 (省略可能。デフォルトは `currentDatabase()`) :<br />    - データベース名<br />    - データベース名を表す文字列を返す定数式 (例: `currentDatabase()`) <br />    - `REGEXP(expression)`。ここで `expression` は DB 名に一致させるための正規表現です。 |
| `tables_regexp` | 指定した DB または複数の DB 内のテーブル名に一致させるための正規表現です。                                                                                                                                                      |

<div id="related">
  ## 関連情報
</div>

* [Merge](../../engines/table-engines/special/merge.md)テーブルエンジン