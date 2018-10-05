<a name="table_engines-graphitemergetree"></a>

# GraphiteMergeTree

This engine is designed for rollup (thinning and aggregating/averaging) [Graphite](http://graphite.readthedocs.io/en/latest/index.html) data. It may be helpful to developers who want to use ClickHouse as a data store for Graphite.

Graphite stores full data in ClickHouse, and data can be retrieved in the following ways:

- Without thinning.

    Uses the [MergeTree](mergetree.md#table_engines-mergetree) engine.

- With thinning.

    Using the `GraphiteMergeTree` engine.

The engine inherits properties from MergeTree. The settings for thinning data are defined by the [graphite_rollup](../server_settings/settings.md#server_settings-graphite_rollup) parameter in the server configuration.

## Using The Engine

The Graphite data table must contain the following fields at minimum:

- `Path` – The metric name (Graphite sensor).
- `Time` – The time for measuring the metric.
- `Value` – The value of the metric at the time set in Time.
- `Version` – Determines which value of the metric with the same Path and Time will remain in the database.

Rollup pattern:

```text
pattern
    regexp
    function
    age -> precision
    ...
pattern
    ...
default
    function
       age -> precision
    ...
```

When processing a record, ClickHouse will check the rules in the `pattern`clause. If the metric name matches the `regexp`, the rules from `pattern` are applied; otherwise, the rules from `default` are used.

Fields in the pattern.

- `age` – The minimum age of the data in seconds.
- `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.
- `precision`– How precisely to define the age of the data in seconds.
- `regexp`– A pattern for the metric name.

Example of settings:

```xml
<graphite_rollup>
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

