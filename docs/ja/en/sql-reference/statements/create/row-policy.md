---
description: 'ROW POLICY のドキュメント'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'リファレンス'
---

[行ポリシー](../../../guides/sre/user-management/index.md#row-policy-management)、つまりユーザーがテーブルから読み取れる行を判定するためのフィルターを作成します。

:::tip
行ポリシーが有効なのは、readonly アクセス権を持つユーザーに対してのみです。ユーザーがテーブルを変更したり、テーブル間でパーティションをコピーしたりできる場合、行ポリシーによる制限は意味をなさなくなります。
:::

構文:

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## USING 句
</div>

行を絞り込む条件を指定できます。各行についてこの条件の評価結果が 0 以外であれば、その行はユーザーに表示されます。

<div id="to-clause">
  ## TO 句
</div>

`TO` セクションでは、このポリシーの適用対象となるユーザーとロールの一覧を指定できます。たとえば、`CREATE ROW POLICY ... TO accountant, john@localhost` のように指定します。

キーワード `ALL` は、現在のユーザーを含むすべての ClickHouse ユーザーを意味します。キーワード `ALL EXCEPT` を使うと、すべてのユーザーの一覧から特定のユーザーを除外できます。たとえば、`CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost` のように指定します。

<div id="as-clause">
  ## AS 句
</div>

同じユーザーに対して、同じテーブルで複数のポリシーを同時に有効にできます。そのため、複数のポリシーの条件を組み合わせる方法が必要になります。

デフォルトでは、ポリシーはブール値の `OR` 演算子で組み合わされます。たとえば、次のようなポリシーがあります。

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

ユーザー `peter` が `b=1` または `c=2` のいずれかに該当する行を参照できるようにします。

`AS` 句は、ポリシーをほかのポリシーとどのように組み合わせるかを指定します。ポリシーには、許可型と制限型の 2 種類があります。デフォルトではポリシーは許可型であり、ブール値演算子 `OR` を使って組み合わされます。

別の方法として、ポリシーを制限型として定義することもできます。制限型ポリシーは、ブール値演算子 `AND` を使って組み合わされます。

一般的な式は次のとおりです。

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

たとえば、次のようなポリシーがあります。

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

ユーザー `peter` が `b=1` かつ `c=2` の両方を満たす行のみを参照できるようにします。

データベースポリシーはテーブルポリシーと組み合わせて適用されます。

たとえば、次のポリシーです。

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

ユーザー `peter` が table1 の行を参照できるのは、`b=1` AND `c=2` の両方を満たす場合のみですが、
mydb 内の他のどのテーブルでも、そのユーザーには `b=1` のポリシーのみが適用されます。

<div id="on-cluster-clause">
  ## ON CLUSTER 句
</div>

クラスター上に行ポリシーを作成できます。詳しくは [Distributed DDL](../../../sql-reference/distributed-ddl.md) を参照してください。

<div id="examples">
  ## 例
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`