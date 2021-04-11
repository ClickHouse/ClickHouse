---
toc_priority: 30
toc_title: MaterializePostgreSQL
---

# MaterializePostgreSQL {#materialize-postgresql}

## Creating a Database {#creating-a-database}

## Requirements {#requirements}

- Each replicated table must have one of the following **replica identity**:

1. **default** (primary key)

2. **index**

``` bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```


Primary key is always checked first. If it is absent, then index, defined as replica identity index, is checked.
If index is used as replica identity, there has to be only one such index in a table.
You can check what type is used for a specific table with the following command:

``` bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

- Setting `wal_level`to `logical` and `max_replication_slots` to at least `2` in the postgresql config file.

