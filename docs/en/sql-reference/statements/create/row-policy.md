---
toc_priority: 41
toc_title: ROW POLICY
---

# CREATE ROW POLICY {#create-row-policy-statement}

Creates a [row policy](../../../operations/access-rights.md#row-policy-management), i.e. a filter used to determine which rows a user can read from a table.

!!! note "Warning"
    Row policies makes sense only for users with readonly access. If user can modify table or copy partitions between tables, it defeats the restrictions of row policies.

Syntax:

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1 
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2 ...] 
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

## USING Clause {#create-row-policy-using}

Allows to specify a condition to filter rows. An user will see a row if the condition is calculated to non-zero for the row.

## TO Clause {#create-row-policy-to}

In the section `TO` you can provide a list of users and roles this policy should work for. For example, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Keyword `ALL` means all the ClickHouse users including current user. Keyword `ALL EXCEPT` allow to exclude some users from the all users list, for example, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

!!! note "Note"
    If there are no row policies defined for a table then any user can `SELECT` all the row from the table. Defining one or more row policies for the table makes the access to the table depending on the row policies no matter if those row policies are defined for the current user or not. For example, the following policy
    
    `CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter`

    forbids the users `mira` and `peter` to see the rows with `b != 1`, and any non-mentioned user (e.g., the user `paul`) will see no rows from `mydb.table1` at all.
    
    If that's not desirable it can't be fixed by adding one more row policy, like the following:

    `CREATE ROW POLICY pol2 ON mydb.table1 USING 1 TO ALL EXCEPT mira, peter`

## AS Clause {#create-row-policy-as}

It's allowed to have more than one policy enabled on the same table for the same user at the one time. So we need a way to combine the conditions from multiple policies.

By default policies are combined using the boolean `OR` operator. For example, the following policies

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

enables the user `peter` to see rows with either `b=1` or `c=2`.

The `AS` clause specifies how policies should be combined with other policies. Policies can be either permissive or restrictive. By default policies are permissive, which means they are combined using the boolean `OR` operator.

A policy can be defined as restrictive as an alternative. Restrictive policies are combined using the boolean `AND` operator.

Here is the general formula:

```
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

For example, the following policies

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

enables the user `peter` to see rows only if both `b=1` AND `c=2`.

## ON CLUSTER Clause {#create-row-policy-on-cluster}

Allows creating row policies on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).


## Examples

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`
