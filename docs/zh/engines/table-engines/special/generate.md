---
toc_priority: 46
toc_title: 随机数生成
---

# 随机数生成表引擎 {#table_engines-generate}

随机数生成表引擎为指定的表模式生成随机数

使用示例:
- 测试时生成可复写的大表
- 为复杂测试生成随机输入

## CH服务端的用法 {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

生成数据时，通过`max_array_length` 设置array列的最大长度， `max_string_length`设置string数据的最大长度

该引擎仅支持 `SELECT` 查询语句.

该引擎支持能在表中存储的所有数据类型 [DataTypes](../../../sql-reference/data-types/index.md) ，除了 `LowCardinality` 和 `AggregateFunction`.

## 示例 {#example}

**1.** 设置 `generate_engine_table` 引擎表:

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

## 实现细节 {#details-of-implementation}

-   以下特性不支持:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   Indices
    -   Replication

[原始文档](https://clickhouse.com/docs/en/operations/table_engines/generate/) <!--hide-->
