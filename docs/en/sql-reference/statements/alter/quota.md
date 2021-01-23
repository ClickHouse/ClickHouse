---
toc_priority: 46
toc_title: QUOTA
---

# ALTER QUOTA {#alter-quota-statement}

Changes [quotas](../../../operations/access-rights.md#quotas-management).

Syntax:

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {NONE | USER_NAME | IP_ADDRESS | CLIENT_KEY | CLIENT_KEY, USER_NAME | CLIENT_KEY, IP_ADDRESS} | NOT KEYED]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT_ROWS | RESULT_BYTES | READ_ROWS | READ_BYTES | EXECUTION_TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```
Multiword key types may be written either with underscores (`CLIENT_KEY`), or with spaces and in simple quotes (`'client key'`). You may also use `'client key or user name'` instead of `CLIENT_KEY, USER_NAME`, and `'client key or ip address'` instead of `CLIENT_KEY, IP_ADDRESS`.

Multiword resource types may be written either with underscores (`RESULT_ROWS`) or without them (`RESULT ROWS`). 

**Examples**

Limit the maximum number of queries for the current user with 123 queries in 15 months constraint:

``` sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER;
```

For the default user limit the maximum execution time with half a second in 30 minutes, and limit the maximum number of queries with 321 and the maximum number of errors with 10 in 5 quaters:

``` sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 MINUTE MAX EXECUTION_TIME = 0.5, FOR INTERVAL 5 QUATER MAX QUERIES = 321, ERRORS = 10 TO default;
```
