---
slug: /en/engines/table-engines/special/time_series
sidebar_position: 60
sidebar_label: TimeSeries
---

# TimeSeries Engine [Experimental]

A table engine storing time series, i.e. a set of values associated with timestamps and tags (or labels):

```
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the TimeSeries table engine
with [allow_experimental_time_series_table](../../../operations/settings/settings.md#allow-experimental-time-series-table) setting.
Input the command `set allow_experimental_time_series_table = 1`.
:::

## Syntax {#syntax}

``` sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[DATA db.data_table_name | DATA ENGINE data_table_engine(arguments)]
[TAGS db.tags_table_name | TAGS ENGINE tags_table_engine(arguments)]
[METRICS db.metrics_table_name | METRICS ENGINE metrics_table_engine(arguments)]
```

## Usage {#usage}

It's easier to start with everything set by default (it's allowed to create a `TimeSeries` table without specifying a list of columns):

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Then this table can be used with the following protocols (a port must be assigned in the server configuration):
- [prometheus remote-write](../../../interfaces/prometheus.md#remote-write)
- [prometheus remote-read](../../../interfaces/prometheus.md#remote-read)

## Target tables {#target-tables}

A `TimeSeries` table doesn't have its own data, everything is stored in its target tables.
This is similar to how a [materialized view](../../../sql-reference/statements/create/view#materialized-view) works,
with the difference that a materialized view has one target table
whereas a `TimeSeries` table has three target tables named [data]{#data-table}, [tags]{#tags-table], and [metrics]{#metrics-table}.

The target tables can be either specified explicitly in the `CREATE TABLE` query
or the `TimeSeries` table engine can generate inner target tables automatically.

The target tables are the following:
1. The _data_ table {#data-table} contains time series associated with some identifier.
The _data_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any | Identifies a combination of a metric names and tags |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)` | A time point |
| `value` | [x] | `Float64` | `Float32` or `Float64` | A value associated with the `timestamp` |

2. The _tags_ table {#tags-table} contains identifiers calculated for each combination of a metric name and tags.
The _tags_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any (must match the type of `id` in the [data]{#data-table} table) | An `id` identifies a combination of a metric name and tags. The DEFAULT expression specifies how to calculate such an identifier |
| `metric_name` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The name of a metric |
| `<tag_value_column>` | [ ] | `String` | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))` | The value of a specific tag, the tag's name and the name of a corresponding column are specified in the [tags_to_columns](#settings) setting |
| `tags` | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map of tags excluding the tag `__name__` containing the name of a metric and excluding tags with names enumerated in the [tags_to_columns](#settings) setting |
| `all_tags` | [ ] | `Map(String, String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Ephemeral column, each row is a map of all the tags excluding only the tag `__name__` containing the name of a metric. The only purpose of that column is to be used while calculating `id` |
| `min_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Minimum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |
| `max_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Maximum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |

3. The _metrics_ table {#metrics-table} contains some information about metrics been collected, the types of those metrics and their descriptions.
The _metrics_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `metric_family_name` | [x] | `String` | `String` or `LowCardinality(String)` | The name of a metric family |
| `type` | [x] | `String` | `String` or `LowCardinality(String)` | The type of a metric family, one of "counter", "gauge", "summary", "stateset", "histogram", "gaugehistogram" |
| `unit` | [x] | `String` | `String` or `LowCardinality(String)` | The unit used in a metric |
| `help` | [x] | `String` | `String` or `LowCardinality(String)` | The description of a metric |

Any row inserted into a `TimeSeries` table will be in fact stored in those three target tables.
A `TimeSeries` table contains all those columns from the [data]{#data-table}, [tags]{#tags-table}, [metrics]{#metrics-table} tables.

## Creation {#creation}

There are multiple ways to create a table with the `TimeSeries` table engine.
The simplest statement

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

will actually create the following table (you can see that by executing `SHOW CREATE TABLE my_table`):

``` sql
CREATE TABLE my_table
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String),
    `min_time` Nullable(DateTime64(3)),
    `max_time` Nullable(DateTime64(3)),
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
DATA ENGINE = MergeTree ORDER BY (id, timestamp)
DATA INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
TAGS ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
METRICS ENGINE = ReplacingMergeTree ORDER BY metric_family_name
METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

So the columns were generated automatically and also there are three inner UUIDs in this statement -
one per each inner target table that was created.
(Inner UUIDs are not shown normally until setting
[show_table_uuid_in_table_create_query_if_not_nil](../../../operations/settings/settings#show_table_uuid_in_table_create_query_if_not_nil)
is set.)

Inner target tables have names like `.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
and each target table has columns which is a subset of the columns of the main `TimeSeries` table:

``` sql
CREATE TABLE default.`.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

``` sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
```

``` sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

## Adjusting types of columns {#adjusting-column-types}

You can adjust the types of almost any column of the inner target tables by specifying them explicitly
while defining the main table. For example,

``` sql
CREATE TABLE my_table
(
    timestamp DateTime64(6)
) ENGINE=TimeSeries
```

will make the inner [data]{#data-table} table store timestamp in microseconds instead of milliseconds:

``` sql
CREATE TABLE default.`.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(6),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

## The `id` column {#id-column}

The `id` column contains identifiers, every identifier is calculated for a combination of a metric name and tags.
The DEFAULT expression for the `id` column is an expression which will be used to calculate such identifiers.
Both the type of the `id` column and that expression can be adjusted by specifying them explicitly:

``` sql
CREATE TABLE my_table
(
    id UInt64 DEFAULT sipHash64(metric_name, all_tags)
) ENGINE=TimeSeries
```

## The `tags` and `all_tags` columns {#tags-and-all-tags}

There are two columns containing maps of tags - `tags` and `all_tags`. In this example they mean the same, however they can be different
if setting `tags_to_columns` is used. This setting allows to specify that a specific tag should be stored in a separate column instead of storing
in a map inside the `tags` column:

``` sql
CREATE TABLE my_table ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

This statement will add columns
```
    `instance` String,
    `job` String
```
to the definition of both `my_table` and its inner [tags]{#tags-table} target table. In this case the `tags` column will not contain tags `instance` and `job`,
but the `all_tags` column will contain them. The `all_tags` column is ephemeral and its only purpose to be used in the DEFAULT expression
for the `id` column.

The types of columns can be adjusted by specifying them explicitly:

``` sql
CREATE TABLE my_table (instance LowCardinality(String), job LowCardinality(Nullable(String)))
ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

## Table engines of inner target tables {#inner-table-engines}

By default inner target tables use the following table engines:
- the [data]{#data-table} table uses [MergeTree](../mergetree-family/mergetree);
- the [tags]{#tags-table} table uses [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates, and also because it's required to do aggregation for columns `min_time` and `max_time`;
- the [metrics]{#metrics-table} table uses [ReplacingMergeTree](../mergetree-family/replacingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates.

Other table engines also can be used for inner target tables if it's specified so:

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
DATA ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

## External target tables {#external-target-tables}

It's possible to make a `TimeSeries` table use a manually created table:

``` sql
CREATE TABLE data_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries DATA data_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

## Settings {#settings}

Here is a list of settings which can be specified while defining a `TimeSeries` table:

| Name | Type | Default | Description |
|---|---|---|---|
| `tags_to_columns` | Map | {} | Map specifying which tags should be put to separate columns in the [tags]{#tags-table} table. Syntax: `{'tag1': 'column1', 'tag2' : column2, ...}` |
| `use_all_tags_column_to_generate_id` | Bool | true | When generating an expression to calculate an identifier of a time series, this flag enables using the `all_tags` column in that calculation |
| `store_min_time_and_max_time` | Bool | true | If set to true then the table will store `min_time` and `max_time` for each time series |
| `aggregate_min_time_and_max_time` | Bool | true | When creating an inner target `tags` table, this flag enables using `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` instead of just `Nullable(DateTime64(3))` as the type of the `min_time` column, and the same for the `max_time` column |
| `filter_by_min_time_and_max_time` | Bool | true | If set to true then the table will use the `min_time` and `max_time` columns for filtering time series |

# Functions {#functions}

Here is a list of functions supporting a `TimeSeries` table as an argument:
- [timeSeriesData](../../../sql-reference/table-functions/timeSeriesData.md)
- [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
- [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)
