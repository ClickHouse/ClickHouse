---
description: 'PARALLEL WITH 句に関するドキュメント'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'PARALLEL WITH 句'
doc_type: 'reference'
---

複数のステートメントを並列に実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

ステートメント `statement1`、`statement2`、`statement3`、... を相互に並列に実行します。これらのステートメントの出力は破棄されます。

多くの場合、ステートメントを並列に実行すると、同じステートメントを順番に実行するよりも高速です。たとえば、`statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` は、`statement1; statement2; statement3` よりも高速になる可能性があります。

<div id="examples">
  ## 例
</div>

2つのテーブルを並列に作成します：

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

2つのテーブルを並列に削除します：

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## 設定
</div>

設定 [max&#95;threads](../../operations/settings/settings.md#max_threads) は、生成するスレッド数を制御します。

<div id="comparison-with-union">
  ## UNION との比較
</div>

`PARALLEL WITH` 句は [UNION](select/union.md) と少し似ており、`UNION` もオペランドを並列に実行します。ただし、いくつか違いがあります。

* `PARALLEL WITH` はオペランドの実行結果を返さず、いずれかで例外が発生した場合にそれを再スローできるだけです。
* `PARALLEL WITH` では、オペランドが同じ結果カラムのセットを持っている必要はありません。
* `PARALLEL WITH` は任意のステートメントを実行できます (`SELECT` に限りません) 。