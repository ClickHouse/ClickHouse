---
toc_priority: 41
toc_title: ROW POLICY
---

# CREATE ROW POLICY {#create-row-policy-statement}

Creates a [filter for rows](../../../operations/access-rights.md#row-policy-management), which a user can read from a table.

Syntax:

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

`ON CLUSTER` clause allows creating row policies on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## AS Clause {#create-row-policy-as}

Using this section you can create permissive or restrictive policies.

Permissive policy grants access to rows. Permissive policies which apply to the same table are combined together using the boolean `OR` operator. Policies are permissive by default.

Restrictive policy restricts access to rows. Restrictive policies which apply to the same table are combined together using the boolean `AND` operator.

Restrictive policies apply to rows that passed the permissive filters. If you set restrictive policies but no permissive policies, the user can’t get any row from the table.

## TO Clause {#create-row-policy-to}

In the section `TO` you can provide a mixed list of roles and users, for example, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Keyword `ALL` means all the ClickHouse users including current user. Keywords `ALL EXCEPT` allow to exclude some users from the all users list, for example, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

## Examples {#examples}

`CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`
