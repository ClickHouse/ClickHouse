# 加入我们 {#join}

加载好的 JOIN 表数据会常驻内存中。

    Join(ANY|ALL, LEFT|INNER, k1[, k2, ...])

引擎参数：`ANY|ALL` – 连接修饰；`LEFT|INNER` – 连接类型。更多信息可参考 [JOIN子句](../../../engines/table-engines/special/join.md#select-join)。
这些参数设置不用带引号，但必须与要 JOIN 表匹配。 k1，k2，……是 USING 子句中要用于连接的关键列。

此引擎表不能用于 GLOBAL JOIN 。

类似于 Set 引擎，可以使用 INSERT 向表中添加数据。设置为 ANY 时，重复键的数据会被忽略（仅一条用于连接）。设置为 ALL 时，重复键的数据都会用于连接。不能直接对 JOIN 表进行 SELECT。检索其数据的唯一方法是将其作为 JOIN 语句右边的表。

跟 Set 引擎类似，Join 引擎把数据存储在磁盘中。

### 限制和设置 {#join-limitations-and-settings}

创建表时，将应用以下设置:

-   join\_use\_nulls
-   max\_rows\_in\_join
-   max\_bytes\_in\_join
-   join\_overflow\_mode
-   join\_any\_take\_last\_row

该 `Join`-发动机表不能用于 `GLOBAL JOIN` 操作。

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/join/) <!--hide-->
