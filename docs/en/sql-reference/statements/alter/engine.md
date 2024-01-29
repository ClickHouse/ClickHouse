---
slug: /en/sql-reference/statements/alter/engine
sidebar_label: ENGINE
---

# ALTER TABLE ... MODIFY ENGINE Statement

``` sql
ALTER TABLE [db.]table MODIFY ENGINE TO [NOT] REPLICATED 
```

This query converts MergeTree family tables between replicated and not replicated engines. During the execution of the query, the table will be temporarily detached and attached with the new engine.

Note that ReplicatedMergeTree table will be created with values of `default_replica_path` and `default_replica_name` settings.

The `allow_modify_engine_query` setting must be enabled.

This query might be useful if you have a MergeTree table on a single server and you want to replicate it across multiple servers.

**Example:**

```sql
CREATE TABLE foo( A Int64, D Date, S String ) 
ENGINE = MergeTree 
PARTITION BY toYYYYMM(D) ORDER BY A;

ALTER TABLE foo MODIFY ENGINE TO REPLICATED 
SETTINGS allow_modify_engine_query = 1;

-- We need zookeeper path to create other replicas
SELECT zookeeper_path FROM system.replicas WHERE table = 'foo';
┌─zookeeper_path─────────────────────────────────────────────┐
│ /clickhouse/tables/95cb19a7-fb87-43f1-8204-71bb7d53dbac/01 │
└────────────────────────────────────────────────────────────┘

-- Now we can create table on cluster
-- This query will crash on current server, but will be completed on other servers
CREATE TABLE foo ON CLUSTER '{cluster}' AS foo 
ENGINE = ReplicatedMergeTree('/clickhouse/tables/95cb19a7-fb87-43f1-8204-71bb7d53dbac/01', '{replica}') 
PARTITION BY toYYYYMM(D) ORDER BY A;
```
