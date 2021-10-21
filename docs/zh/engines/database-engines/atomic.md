---
toc_priority: 32
toc_title: Atomic
---


# Atomic {#atomic}

它支持非阻塞 DROP 和 RENAME TABLE 查询以及原子 EXCHANGE TABLES t1 AND t2 查询。默认情况下使用Atomic数据库引擎。

## 创建数据库 {#creating-a-database}

```sql
CREATE DATABASE test ENGINE = Atomic;
```

[原文](https://clickhouse.tech/docs/en/engines/database_engines/atomic/) <!--hide-->
