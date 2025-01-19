---
slug: /ja/sql-reference/statements/create/row-policy
sidebar_position: 41
sidebar_label: ROW POLICY
title: "CREATE ROW POLICY"
---

[行ポリシー](../../../guides/sre/user-management/index.md#row-policy-management)を作成します。これは、ユーザーがテーブルから読み取ることができる行を決定するために使用されるフィルターです。

:::tip
行ポリシーは、読み取り専用アクセスを持つユーザーに対してのみ意味があります。ユーザーがテーブルを変更したり、テーブル間でパーティションをコピーしたりできる場合、行ポリシーの制限は無効になります。
:::

構文:

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

## USING 句

行をフィルタリングする条件を指定できます。ある行に対して条件が非ゼロに計算された場合、その行はユーザーに表示されます。

## TO 句

`TO` セクションでは、このポリシーが適用されるユーザーやロールのリストを指定できます。例えば、`CREATE ROW POLICY ... TO accountant, john@localhost` のようにします。

キーワード `ALL` は、現在のユーザーを含むすべての ClickHouse ユーザーを意味します。キーワード `ALL EXCEPT` を使用すると、特定のユーザーを除外することができます。たとえば、`CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

:::note
テーブルに定義されている行ポリシーがない場合、任意のユーザーがテーブルからすべての行を `SELECT` できます。テーブルに1つ以上の行ポリシーを定義すると、現在のユーザーに関係なく、そのテーブルへのアクセスは行ポリシーに依存するようになります。例えば、以下のポリシー:

`CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter`

は、ユーザー `mira` と `peter` が `b != 1` の行を見ることを禁止しますが、言及されていないユーザー (例: ユーザー `paul`) は `mydb.table1` の行を全く見ることができません。

それが望ましくない場合、以下のようなもう1つの行ポリシーを追加することで修正できます:

`CREATE ROW POLICY pol2 ON mydb.table1 USING 1 TO ALL EXCEPT mira, peter`
:::

## AS 句

同じユーザーに対して、同じテーブルで複数のポリシーを同時に有効化することができます。そのため、複数のポリシーに基づく条件を結合する方法が必要です。

デフォルトでは、ポリシーは論理演算子 `OR` を使用して結合されます。例えば、以下のポリシー:

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

は、ユーザー `peter` が `b=1` または `c=2` のいずれかの行を閲覧できるようにします。

`AS` 句は、ポリシーを他のポリシーとどのように組み合わせるかを指定します。ポリシーは[許可的](PERMISSIVE)または[制限的](RESTRICTIVE)にできます。デフォルトでは、ポリシーは許可的で、論理演算子 `OR` を使って結合されます。

代替としてポリシーを制限的に定義することができます。制限的ポリシーは論理演算子 `AND` を使って結合されます。

一般的な公式は次のとおりです:

```
row_is_visible = (1つ以上の許可的なポリシーの条件が非ゼロ) AND
                 (すべての制限的なポリシーの条件が非ゼロ)
```

例えば、以下のポリシー:

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

は、ユーザー `peter` が `b=1` かつ `c=2` の行のみを見ることを許可します。

データベースポリシーはテーブルポリシーと結合されます。

例えば、以下のポリシー:

``` sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

は、ユーザー `peter` が `b=1` かつ `c=2` の場合のみ table1 の行を見ることができますが、mydb 内の他のテーブルには `b=1` ポリシーのみが適用されます。

## ON CLUSTER 句

クラスターに行ポリシーを作成することができます。[分散DDL](../../../sql-reference/distributed-ddl.md)を参照してください。

## 例

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`
