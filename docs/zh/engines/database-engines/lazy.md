---
toc_priority: 31
toc_title: "延时引擎"
---

# 延时引擎Lazy {#lazy}

在距最近一次访问间隔`expiration_time_in_seconds`时间段内，将表保存在内存中，仅适用于 \*Log引擎表

由于针对这类表的访问间隔较长，对保存大量小的 \*Log引擎表进行了优化，

## 创建数据库 {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[原始文章](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
