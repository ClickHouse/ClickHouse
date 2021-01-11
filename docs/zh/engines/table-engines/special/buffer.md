# 缓冲区 {#buffer}

缓冲数据写入 RAM 中，周期性地将数据刷新到另一个表。在读取操作时，同时从缓冲区和另一个表读取数据。

    Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)

引擎的参数：database，table - 要刷新数据的表。可以使用返回字符串的常量表达式而不是数据库名称。 num_layers - 并行层数。在物理上，该表将表示为 num_layers 个独立缓冲区。建议值为16。min_time，max_time，min_rows，max_rows，min_bytes，max_bytes - 从缓冲区刷新数据的条件。

如果满足所有 «min» 条件或至少一个 «max» 条件，则从缓冲区刷新数据并将其写入目标表。min_time，max_time — 从第一次写入缓冲区时起以秒为单位的时间条件。min_rows，max_rows - 缓冲区中行数的条件。min_bytes，max_bytes - 缓冲区中字节数的条件。

写入时，数据从 num_layers 个缓冲区中随机插入。或者，如果插入数据的大小足够大（大于 max_rows 或 max_bytes ），则会绕过缓冲区将其写入目标表。

每个 «num_layers» 缓冲区刷新数据的条件是分别计算。例如，如果 num_layers = 16 且 max_bytes = 100000000，则最大RAM消耗将为1.6 GB。

示例：

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

创建一个 «merge.hits_buffer» 表，其结构与 «merge.hits» 相同，并使用 Buffer 引擎。写入此表时，数据缓冲在 RAM 中，然后写入 «merge.hits» 表。创建了16个缓冲区。如果已经过了100秒，或者已写入100万行，或者已写入100 MB数据，则刷新每个缓冲区的数据；或者如果同时已经过了10秒并且已经写入了10,000行和10 MB的数据。例如，如果只写了一行，那么在100秒之后，都会被刷新。但是如果写了很多行，数据将会更快地刷新。

当服务器停止时，使用 DROP TABLE 或 DETACH TABLE，缓冲区数据也会刷新到目标表。

可以为数据库和表名在单个引号中设置空字符串。这表示没有目的地表。在这种情况下，当达到数据刷新条件时，缓冲器被简单地清除。这可能对于保持数据窗口在内存中是有用的。

从 Buffer 表读取时，将从缓冲区和目标表（如果有）处理数据。
请注意，Buffer 表不支持索引。换句话说，缓冲区中的数据被完全扫描，对于大缓冲区来说可能很慢。（对于目标表中的数据，将使用它支持的索引。）

如果 Buffer 表中的列集与目标表中的列集不匹配，则会插入两个表中存在的列的子集。

如果类型与 Buffer 表和目标表中的某列不匹配，则会在服务器日志中输入错误消息并清除缓冲区。
如果在刷新缓冲区时目标表不存在，则会发生同样的情况。

如果需要为目标表和 Buffer 表运行 ALTER，我们建议先删除 Buffer 表，为目标表运行 ALTER，然后再次创建 Buffer 表。

如果服务器异常重启，缓冲区中的数据将丢失。

PREWHERE，FINAL 和 SAMPLE 对缓冲表不起作用。这些条件将传递到目标表，但不用于处理缓冲区中的数据。因此，我们建议只使用Buffer表进行写入，同时从目标表进行读取。

将数据添加到缓冲区时，其中一个缓冲区被锁定。如果同时从表执行读操作，则会导致延迟。

插入到 Buffer 表中的数据可能以不同的顺序和不同的块写入目标表中。因此，Buffer 表很难用于正确写入 CollapsingMergeTree。为避免出现问题，您可以将 «num_layers» 设置为1。

如果目标表是复制表，则在写入 Buffer 表时会丢失复制表的某些预期特征。数据部分的行次序和大小的随机变化导致数据不能去重，这意味着无法对复制表进行可靠的 «exactly once» 写入。

由于这些缺点，我们只建议在极少数情况下使用 Buffer 表。

当在单位时间内从大量服务器接收到太多 INSERTs 并且在插入之前无法缓冲数据时使用 Buffer 表，这意味着这些 INSERTs 不能足够快地执行。

请注意，一次插入一行数据是没有意义的，即使对于 Buffer 表也是如此。这将只产生每秒几千行的速度，而插入更大的数据块每秒可以产生超过一百万行（参见 «性能» 部分）。

[原始文章](https://clickhouse.tech/docs/zh/operations/table_engines/buffer/) <!--hide-->
