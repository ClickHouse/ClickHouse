---
sidebar_position: 90
sidebar_label:  GraphiteMergeTree
---

# GraphiteMergeTree

This engine is designed for thinning and aggregating/averaging (rollup) [Graphite](http://graphite.readthedocs.io/en/latest/index.html) data. It may be helpful to developers who want to use ClickHouse as a data store for Graphite.

You can use any ClickHouse table engine to store the Graphite data if you do not need rollup, but if you need a rollup use `GraphiteMergeTree`. The engine reduces the volume of storage and increases the efficiency of queries from Graphite.

The engine inherits properties from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

## Creating a Table {#creating-table}

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

See a detailed description of the [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) query.

A table for the Graphite data should have the following columns for the following data:

-   Metric name (Graphite sensor). Data type: `String`.

-   Time of measuring the metric. Data type: `DateTime`.

-   Value of the metric. Data type: any numeric.

-   Version of the metric. Data type: any numeric (ClickHouse saves the rows with the highest version or the last written if versions are the same. Other rows are deleted during the merge of data parts).

The names of these columns should be set in the rollup configuration.

**GraphiteMergeTree parameters**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**Query clauses**

When creating a `GraphiteMergeTree` table, the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::warning
Do not use this method in new projects and, if possible, switch old projects to the method described above.
:::

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

All of the parameters excepting `config_section` have the same meaning as in `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## Rollup Configuration {#rollup-configuration}

The settings for rollup are defined by the [graphite_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) parameter in the server configuration. The name of the parameter could be any. You can create several configurations and use them for different tables.

Rollup configuration structure:

      required-columns
      patterns

### Required Columns {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. Default value: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### Patterns {#patterns}

Structure of the `patterns` section:

``` text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
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

:::warning
Patterns must be strictly ordered:

1. Patterns without `function` or `retention`.
1. Patterns with both `function` and `retention`.
1. Pattern `default`.
:::

When processing a row, ClickHouse checks the rules in the `pattern` sections. Each of `pattern` (including `default`) sections can contain `function` parameter for aggregation, `retention` parameters or both. If the metric name matches the `regexp`, the rules from the `pattern` section (or sections) are applied; otherwise, the rules from the `default` section are used.

Fields for `pattern` and `default` sections:

-   `rule_type` - a rule's type. It's applied only to a particular metrics. The engine use it to separate plain and tagged metrics. Optional parameter. Default value: `all`.
It's unnecessary when performance is not critical, or only one metrics type is used, e.g. plain metrics. By default only one type of rules set is created. Otherwise, if any of special types is defined, two different sets are created. One for plain metrics (root.branch.leaf) and one for tagged metrics (root.branch.leaf;tag1=value1).
The default rules are ended up in both sets.
Valid values:
    -   `all` (default) - a universal rule, used when `rule_type` is omitted.
    -   `plain` - a rule for plain metrics. The field `regexp` is processed as regular expression.
    -   `tagged` - a rule for tagged metrics (metrics are stored in DB in the format of `someName?tag1=value1&tag2=value2&tag3=value3`). Regular expression must be sorted by tags' names, first tag must be `__name__` if exists. The field `regexp` is processed as regular expression.
    -   `tag_list` - a rule for tagged matrics, a simple DSL for easier metric description in graphite format `someName;tag1=value1;tag2=value2`, `someName`, or `tag1=value1;tag2=value2`. The field `regexp` is translated into a `tagged` rule. The sorting by tags' names is unnecessary, ti will be done automatically. A tag's value (but not a name) can be set as a regular expression, e.g. `env=(dev|staging)`.
-   `regexp` – A pattern for the metric name (a regular or DSL).
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`. Accepted functions: min / max / any / avg. The average is calculated imprecisely, like the average of the averages.

### Configuration Example without rules types {#configuration-example}

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

### Configuration Example with rules types {#configuration-typed-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
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
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
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


:::warning
Data rollup is performed during merges. Usually, for old partitions, merges are not started, so for rollup it is necessary to trigger an unscheduled merge using [optimize](../../../sql-reference/statements/optimize.md). Or use additional tools, for example [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::
