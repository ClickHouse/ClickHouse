# 布尔值 {#boolean-values}

从 https://github.com/ClickHouse/ClickHouse/commit/4076ae77b46794e73594a9f400200088ed1e7a6e 之后，有单独的类型来存储布尔值。

在此之前的版本，没有单独的类型来存储布尔值。可以使用 UInt8 类型，取值限制为 0 或 1。
