# ClickHouse可以考虑缺点的功能

1. 没有完整的交易。
2. 缺乏以高速率和低延迟修改或删除已插入数据的能力。 有批次删除和更新可用于清理或修改数据，例如符合[GDPR](https://gdpr-info.eu)。
3. 稀疏索引使得ClickHouse不适合通过其键检索单行的点查询。

[来源文章](https://clickhouse.yandex/docs/zh/introduction/features_considered_disadvantages/) <!--hide-->
