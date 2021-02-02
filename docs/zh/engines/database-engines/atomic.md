---
toc_priority: 32
toc_title: Atomic
---


# Atomic {#atomic}

It is supports non-blocking `DROP` and `RENAME TABLE` queries and atomic `EXCHANGE TABLES t1 AND t2` queries. Atomic database engine is used by default.

## Creating a Database {#creating-a-database}

```sql
CREATE DATABASE test ENGINE = Atomic;
```

[Original article](https://clickhouse.tech/docs/en/engines/database_engines/atomic/) <!--hide-->
