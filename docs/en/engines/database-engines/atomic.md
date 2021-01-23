---
toc_priority: 32
toc_title: Atomic
---


# Atomic {#atomic}

It is an experimental database engine, it supports non-blocking `DROP` and `RENAME TABLE` queries and atomic `EXCHANGE TABLES t1 AND t2` queries.

## Creating a Database {#creating-a-database}

    CREATE DATABASE test ENGINE = Atomic;

[Original article](https://clickhouse.tech/docs/en/engines/database_engines/atomic/) <!--hide-->
