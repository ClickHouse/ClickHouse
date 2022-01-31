---
toc_priority: 52
toc_title: TRUNCATE
---

# TRUNCATE 语句 {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

删除表中的所有数据。当省略子句 `IF EXISTS` 时，如果表不存在，则查询返回一个错误。



`TRUNCATE` 查询不支持[View](../../engines/table-engines/special/view.md),[File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md), [Buffer](../../engines/table-engines/special/buffer.md) 和 [Null](../../engines/table-engines/special/null.md)表引擎。



可以使用 replication_alter_partitions_sync 设置在复制集上等待执行的操作。



通过 replication_wait_for_inactive_replica_timeout 设置，可以指定不活动副本执行 `TRUNCATE`查询需要等待多长时间(以秒为单位)。



!!! info  "注意"
    如果`replication_alter_partitions_sync` 被设置为`2`，并且某些复制集超过 `replication_wait_for_inactive_replica_timeout`设置的时间不激活，那么将抛出一个异常`UNFINISHED`。


