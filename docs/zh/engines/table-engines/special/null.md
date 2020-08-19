# Null {#null}

当写入 Null 类型的表时，将忽略数据。从 Null 类型的表中读取时，返回空。

但是，可以在 Null 类型的表上创建物化视图。写入表的数据将转发到视图中。

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/null/) <!--hide-->
