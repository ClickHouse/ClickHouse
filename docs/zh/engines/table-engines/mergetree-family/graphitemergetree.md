---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 38
toc_title: GraphiteMergeTree
---

# GraphiteMergeTree {#graphitemergetree}

此引擎专为细化和聚合/平均（rollup) [石墨](http://graphite.readthedocs.io/en/latest/index.html) 戴达 对于想要使用ClickHouse作为Graphite的数据存储的开发人员来说，这可能会有所帮助。

您可以使用任何ClickHouse表引擎来存储石墨数据，如果你不需要汇总，但如果你需要一个汇总使用 `GraphiteMergeTree`. 该引擎减少了存储量，并提高了Graphite查询的效率。

引擎继承从属性 [MergeTree](mergetree.md).

## 创建表 {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

请参阅的详细说明 [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) 查询。

Graphite数据的表应具有以下数据的列:

-   公制名称（石墨传感器）。 数据类型: `String`.

-   测量度量的时间。 数据类型: `DateTime`.

-   度量值。 数据类型：任何数字。

-   指标的版本。 数据类型：任何数字。

    如果版本相同，ClickHouse会保存版本最高或最后写入的行。 其他行在数据部分合并期间被删除。

应在汇总配置中设置这些列的名称。

**GraphiteMergeTree参数**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**查询子句**

当创建一个 `GraphiteMergeTree` 表，相同 [条款](mergetree.md#table_engine-mergetree-creating-a-table) 是必需的，因为当创建 `MergeTree` 桌子

<details markdown="1">

<summary>不推荐使用的创建表的方法</summary>

!!! attention "注意"
    不要在新项目中使用此方法，如果可能的话，请将旧项目切换到上述方法。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

所有参数除外 `config_section` 具有相同的含义 `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## 汇总配置 {#rollup-configuration}

汇总的设置由 [graphite\_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) 服务器配置中的参数。 参数的名称可以是any。 您可以创建多个配置并将它们用于不同的表。

汇总配置结构:

      required-columns
      patterns

### 必填列 {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. 默认值: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### 模式 {#patterns}

的结构 `patterns` 科:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

!!! warning "注意"
    模式必须严格排序:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

在处理行时，ClickHouse会检查以下内容中的规则 `pattern` 部分。 每个 `pattern` （包括 `default`）部分可以包含 `function` 聚合参数, `retention` 参数或两者兼而有之。 如果指标名称匹配 `regexp`，从规则 `pattern` 部分（sections节）的应用;否则，从规则 `default` 部分被使用。

字段为 `pattern` 和 `default` 科:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### 配置示例 {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
