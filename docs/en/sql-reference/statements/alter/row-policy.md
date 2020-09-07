---
toc_priority: 47
toc_title: ROW POLICY
---

# ALTER ROW POLICY {#alter-row-policy-statement}

Changes row policy.

Syntax:

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```
