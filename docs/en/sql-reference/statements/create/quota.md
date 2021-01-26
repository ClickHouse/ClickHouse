---
toc_priority: 42
toc_title: QUOTA
---

# CREATE QUOTA {#create-quota-statement}

Creates a [quota](../../../operations/access-rights.md#quotas-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {USER_NAME | IP_ADDRESS | CLIENT_KEY | CLIENT_KEY, USER_NAME | CLIENT_KEY, IP_ADDRESS} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT_ROWS | RESULT_BYTES | READ_ROWS | READ_BYTES | EXECUTION_TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

`ON CLUSTER` clause allows creating quotas on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

**Examples** 

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER;
```

For the default user limit the maximum execution time with half a second in 30 minutes, and limit the maximum number of queries with 321 and the maximum number of errors with 10 in 5 quaters:

``` sql
CREATE QUOTA qB FOR INTERVAL 30 MINUTE MAX EXECUTION_TIME = 0.5, FOR INTERVAL 5 QUATER MAX QUERIES = 321, ERRORS = 10 TO default;
```
