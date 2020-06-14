---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

GenerateRandom表引擎为给定的表架构生成随机数据。

使用示例:

-   在测试中使用填充可重复的大表。
-   为模糊测试生成随机输入。

## 在ClickHouse服务器中的使用 {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

该 `max_array_length` 和 `max_string_length` 参数指定所有的最大长度
数组列和字符串相应地在生成的数据中。

生成表引擎仅支持 `SELECT` 查询。

它支持所有 [数据类型](../../../sql-reference/data-types/index.md) 可以存储在一个表中，除了 `LowCardinality` 和 `AggregateFunction`.

**示例:**

**1.** 设置 `generate_engine_table` 表:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** 查询数据:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## 实施细节 {#details-of-implementation}

-   不支持:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   指数
    -   复制

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
