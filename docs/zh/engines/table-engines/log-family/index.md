---
toc_folder_title: "\u65E5\u5FD7\u7CFB\u5217"
toc_title: 日志引擎系列
toc_priority: 29
---

# 日志引擎系列 {#table_engines-log-engine-family}

这些引擎是为了需要写入许多小数据量（少于一百万行）的表的场景而开发的。

这系列的引擎有：

-   [StripeLog](stripelog.md)
-   [日志](log.md)
-   [TinyLog](tinylog.md)

## 共同属性 {#table_engines-log-engine-family-common-properties}

引擎：

-   数据存储在磁盘上。

-   写入时将数据追加在文件末尾。

-   不支持[突变](../../../engines/table-engines/log-family/index.md#alter-mutations)操作。

-   不支持索引。

        这意味着 `SELECT` 在范围查询时效率不高。

-   非原子地写入数据。

        如果某些事情破坏了写操作，例如服务器的异常关闭，你将会得到一张包含了损坏数据的表。

## 差异 {#table_engines-log-engine-family-differences}

`Log` 和 `StripeLog` 引擎支持：

-   并发访问数据的锁。

        `INSERT` 请求执行过程中表会被锁定，并且其他的读写数据的请求都会等待直到锁定被解除。如果没有写数据的请求，任意数量的读请求都可以并发执行。

-   并行读取数据。

        在读取数据时，ClickHouse 使用多线程。 每个线程处理不同的数据块。

`Log` 引擎为表中的每一列使用不同的文件。`StripeLog` 将所有的数据存储在一个文件中。因此 `StripeLog` 引擎在操作系统中使用更少的描述符，但是 `Log` 引擎提供更高的读性能。

`TinyLog` 引擎是该系列中最简单的引擎并且提供了最少的功能和最低的性能。`TinyLog` 引擎不支持并行读取和并发数据访问，并将每一列存储在不同的文件中。它比其余两种支持并行读取的引擎的读取速度更慢，并且使用了和 `Log` 引擎同样多的描述符。你可以在简单的低负载的情景下使用它。

[来源文章](https://clickhouse.com/docs/en/operations/table_engines/log_family/) <!--hide-->
