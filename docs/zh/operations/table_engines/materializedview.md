# 物化视图

Used for implementing materialized views (详情参见 [CREATE TABLE](../../query_language/create.md)). For storing data, it uses a different engine that was specified when creating the view. When reading from a table, it just uses this engine.


[来源文章](https://clickhouse.yandex/docs/en/operations/table_engines/materializedview/) <!--hide-->
