# StripeLog {#stripelog}

该引擎属于日志引擎系列。请在[日志引擎系列](index.md)文章中查看引擎的共同属性和差异。

在你需要写入许多小数据量（小于一百万行）的表的场景下使用这个引擎。

## 建表 {#table_engines-stripelog-creating-a-table}

    CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
    (
        column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
        column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
        ...
    ) ENGINE = StripeLog

查看[建表](../../../engines/table-engines/log-family/stripelog.md#create-table-query)请求的详细说明。

## 写数据 {#table_engines-stripelog-writing-the-data}

`StripeLog` 引擎将所有列存储在一个文件中。对每一次 `Insert` 请求，ClickHouse 将数据块追加在表文件的末尾，逐列写入。

ClickHouse 为每张表写入以下文件：

-   `data.bin` — 数据文件。
-   `index.mrk` — 带标记的文件。标记包含了已插入的每个数据块中每列的偏移量。

`StripeLog` 引擎不支持 `ALTER UPDATE` 和 `ALTER DELETE` 操作。

## 读数据 {#table_engines-stripelog-reading-the-data}

带标记的文件使得 ClickHouse 可以并行的读取数据。这意味着 `SELECT` 请求返回行的顺序是不可预测的。使用 `ORDER BY` 子句对行进行排序。

## 使用示例 {#table_engines-stripelog-example-of-use}

建表：

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

插入数据：

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

我们使用两次 `INSERT` 请求从而在 `data.bin` 文件中创建两个数据块。

ClickHouse 在查询数据时使用多线程。每个线程读取单独的数据块并在完成后独立的返回结果行。这样的结果是，大多数情况下，输出中块的顺序和输入时相应块的顺序是不同的。例如：

``` sql
SELECT * FROM stripe_log_table
```

    ┌───────────timestamp─┬─message_type─┬─message────────────────────┐
    │ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
    │ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
    └─────────────────────┴──────────────┴────────────────────────────┘
    ┌───────────timestamp─┬─message_type─┬─message───────────────────┐
    │ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
    └─────────────────────┴──────────────┴───────────────────────────┘

对结果排序（默认增序）：

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

    ┌───────────timestamp─┬─message_type─┬─message────────────────────┐
    │ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
    │ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
    │ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
    └─────────────────────┴──────────────┴────────────────────────────┘

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
