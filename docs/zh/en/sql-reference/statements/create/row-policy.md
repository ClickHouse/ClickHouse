---
description: 'ROW POLICY 文档'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'reference'
---

创建一个[行策略](../../../guides/sre/user-management/index.md#row-policy-management)，即用于确定用户可以从表中读取哪些行的过滤器。

:::tip
行策略仅对具有 readonly 访问权限的用户才有意义。如果用户可以修改表，或在表之间复制分区，那么行策略的限制就形同虚设。
:::

语法：

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## USING 子句
</div>

允许指定用于过滤行的条件。若某一行上该条件的计算结果为非零，用户即可看到该行。

<div id="to-clause">
  ## TO 子句
</div>

在 `TO` 部分中，你可以指定此策略适用的用户和角色列表。例如，`CREATE ROW POLICY ... TO accountant, john@localhost`。

关键字 `ALL` 表示所有 ClickHouse 用户，包括当前用户。关键字 `ALL EXCEPT` 允许从所有用户列表中排除某些用户，例如，`CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

<div id="as-clause">
  ## AS 子句
</div>

允许在同一时间为同一用户的同一张表启用多个行策略。因此，我们需要一种方法来组合多个行策略中的条件。

默认情况下，这些行策略通过布尔值 `或` 运算符组合。例如，以下行策略：

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

允许用户 `peter` 查看满足 `b=1` 或 `c=2` 的行。

`AS` 子句指定行策略应如何与其他行策略组合。行策略可以是宽松型或限制型。默认情况下，行策略为宽松型，这意味着它们会使用布尔 `或` 运算符进行组合。

行策略也可以作为另一种选择定义为限制型。限制型行策略会使用布尔 `与` 运算符进行组合。

以下是通用公式：

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

例如，下列行策略：

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

仅当同时满足 `b=1` 与 `c=2` 时，用户 `peter` 才能看到这些行。

数据库行策略会与表行策略叠加生效。

例如，以下行策略：

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

使用户 `peter` 仅在同时满足 `b=1` 与 `c=2` 时才能看到 table1 中的行，尽管
mydb 中的任何其他表对该用户都只应用 `b=1` 这一行策略。

<div id="on-cluster-clause">
  ## ON CLUSTER 子句
</div>

允许在集群中创建行策略，参见 [Distributed DDL](../../../sql-reference/distributed-ddl.md)。

<div id="examples">
  ## 示例
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`