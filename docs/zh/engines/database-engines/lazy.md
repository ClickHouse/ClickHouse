---
sidebar_position: 31
sidebar_label: Lazy
---

# Lazy {#lazy}

在最后一次访问之后，只在RAM中保存`expiration_time_in_seconds`秒。只能用于\*Log表。

它是为存储许多小的\*Log表而优化的，对于这些表，访问之间有很长的时间间隔。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);
```

[来源文章](https://clickhouse.com/docs/en/database_engines/lazy/) <!--hide-->
