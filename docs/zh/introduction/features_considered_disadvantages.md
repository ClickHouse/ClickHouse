# ClickHouse可以认为是缺点的功能 {#clickhouseke-yi-ren-wei-shi-que-dian-de-gong-neng}

1.  没有完整的事务支持。
2.  缺少高频率，低延迟的修改或删除已存在数据的能力。仅能用于批量删除或修改数据，但这符合 [GDPR](https://gdpr-info.eu)。
3.  稀疏索引使得ClickHouse不适合通过其键检索单行的点查询。

[来源文章](https://clickhouse.tech/docs/zh/introduction/features_considered_disadvantages/) <!--hide-->
