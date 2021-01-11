---
toc_priority: 42
toc_title: QUOTA
---

# CREATE QUOTA {#create-quota-statement}

Creates a [quota](../../../operations/access-rights.md#quotas-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'forwarded ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

`ON CLUSTER` clause allows creating quotas on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Example {#create-quota-example}

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```
