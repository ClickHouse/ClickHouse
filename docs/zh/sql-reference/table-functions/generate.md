---
toc_priority: 47
toc_title: generateRandom
---

# generateRandom {#generaterandom}

生成具用给定的模式的随机数据。
允许用数据来填充测试表。
支持所有可以存储在表中的数据类型， `LowCardinality` 和 `AggregateFunction`除外。

``` sql
generateRandom('name TypeName[, name TypeName]...', [, 'random_seed'[, 'max_string_length'[, 'max_array_length']]]);
```

**参数**

-   `name` — 对应列的名称。
-   `TypeName` — 对应列的类型。
-   `max_array_length` — 生成数组的最大长度。 默认为10。
-   `max_string_length` — 生成字符串的最大长度。 默认为10。
-   `random_seed` — 手动指定随机种子以产生稳定的结果。 如果为NULL-种子是随机生成的。

**返回值**

具有请求模式的表对象。

## 用法示例 {#usage-example}

``` sql
SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2) LIMIT 3;
```

``` text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/generate/) <!--hide-->
