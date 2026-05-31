---
description: 'A table engine storing time series, i.e. a set of values associated
  with timestamps and tags (or labels).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'TimeSeries table engine'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# TimeSeries table engine

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

A table engine storing time series, i.e. a set of values associated with timestamps and tags (or labels):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the TimeSeries table engine
with [allow_experimental_time_series_table](/operations/settings/settings#allow_experimental_time_series_table) setting.
Input the command `set allow_experimental_time_series_table = 1`.
:::

## Syntax {#syntax}

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
The keyword `SAMPLES` has an alias `DATA` which is kept for backwards compatibility.
:::

## Usage {#usage}

It's easier to start with everything set by default (it's allowed to create a `TimeSeries` table without specifying a list of columns):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Then this table can be used with the following protocols (a port must be assigned in the server configuration):
- [prometheus remote-write](/interfaces/prometheus#remote-write)
- [prometheus remote-read](/interfaces/prometheus#remote-read)

When configuring a Prometheus remote-write handler with `enable_table_name_url_routing`, the URL is expected to start with `/{database}/{table}/`. Make sure the handler's `<url>` rule matches paths that include the database and table name. For example:

```xml
<url>regex:^/[^/]+/[^/]+/write$</url>
<enable_table_name_url_routing>true</enable_table_name_url_routing>
```

### Outer columns {#outer-columns}

Columns of a TimeSeries table are generated automatically. These are outer columns, they store no data, they just provide interface for SELECT/INSERT. Actual data is stored in [target tables](#target-tables). Here is the list of the outer columns:

| Name | Type | Description |
|---|---|---|
| `metric_name` | `String` | The name of the metric |
| `tags` | `Map(String, String)` | Map of tags (labels) for the time series |
| `time_series` | `Array(Tuple(DateTime64(3), Float64))` by default | Array of (timestamp, value) pairs for a time series. The tuple's timestamp and scalar element types can be derived from the samples `INNER COLUMNS` declaration (see [Specifying outer columns](#specifying-outer-columns)) |
| `metric_family` | `String` | The name of the metric family (for metrics metadata) |
| `type` | `String` | The type of the metric (e.g. "counter", "gauge") |
| `unit` | `String` | The unit of the metric |
| `help` | `String` | The description of the metric |

Example:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` is allowed to be empty on insertion, that means the metric name is specified in `tags` under `__name__`, for example:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

To insert metrics metadata, insert into the `metric_family`, `type`, `unit`, and `help` columns:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

### Specifying outer columns {#specifying-outer-columns}

The outer `time_series` column can be listed explicitly in a `CREATE TABLE` statement to override its default `Array(Tuple(DateTime64(3), Float64))` type. ClickHouse extracts the timestamp and scalar types from the tuple and propagates them to the inner samples table:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

This is equivalent to declaring the timestamp and value column types in the samples `INNER COLUMNS` clause directly:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

If both forms are used in the same `CREATE TABLE` statement, the declared types must match.

## Target tables {#target-tables}

A `TimeSeries` table doesn't have its own data, everything is stored in its target tables.
This is similar to how a [materialized view](../../../sql-reference/statements/create/view#materialized-view) works,
with the difference that a materialized view has one target table
whereas a `TimeSeries` table has three target tables named [samples](#samples-table), [tags](#tags-table), and [metrics](#metrics-table).

The target tables can be either specified explicitly in the `CREATE TABLE` query
or the `TimeSeries` table engine can generate inner target tables automatically.

Rows inserted into a `TimeSeries` table are transformed, split into blocks, and inserted in these three target tables.

The target tables are the following:

### Samples table {#samples-table}

The _samples_ table contains time series associated with some identifier.

The _samples_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any | Identifies a combination of a metric names and tags |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)` | A time point |
| `value` | [x] | `Float64` | `Float32` or `Float64` | A value associated with the `timestamp` |

### Tags table {#tags-table}

The _tags_ table contains identifiers calculated for each combination of a metric name and tags.

The _tags_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `id` | [x] | `UUID` | any (must match the type of `id` in the [samples](#samples-table) table) | An `id` identifies a combination of a metric name and tags. The DEFAULT expression specifies how to calculate such an identifier |
| `metric_name` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The name of a metric |
| `<tag_value_column>` | [ ] | `String` | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))` | The value of a specific tag, the tag's name and the name of a corresponding column are specified in the [tags_to_columns](#settings) setting |
| `tags` | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map of tags excluding the tag `__name__` containing the name of a metric and excluding tags with names enumerated in the [tags_to_columns](#settings) setting |
| `all_tags` | [ ] | `Map(String, String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Ephemeral column, each row is a map of all the tags excluding only the tag `__name__` containing the name of a metric. The only purpose of that column is to be used while calculating `id` |
| `min_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Minimum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |
| `max_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` or `Nullable(DateTime64(X))` | Maximum timestamp of time series with that `id`. The column is created if [store_min_time_and_max_time](#settings) is `true` |

### Metrics table {#metrics-table}

The _metrics_ table contains some information about metrics been collected, the types of those metrics and their descriptions.

The _metrics_ table must have columns:

| Name | Mandatory? | Default type | Possible types | Description |
|---|---|---|---|---|
| `metric_family_name` | [x] | `String` | `String` or `LowCardinality(String)` | The name of a metric family |
| `type` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The type of a metric family, one of "counter", "gauge", "summary", "stateset", "histogram", "gaugehistogram" |
| `unit` | [x] | `LowCardinality(String)` | `String` or `LowCardinality(String)` | The unit used in a metric |
| `help` | [x] | `String` | `String` or `LowCardinality(String)` | The description of a metric |

## Creation {#creation}

There are multiple ways to create a table with the `TimeSeries` table engine.
The simplest statement

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

will actually create the following table (you can see that by executing `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

So the columns were generated automatically and also there are three inner target tables with their own column definitions
stored in the `INNER COLUMNS` clauses.

Inner target tables have names like `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
and each target table has its own set of columns:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
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

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

## Creating a table AS existing table {#create-as}

Statement `CREATE TABLE new_table AS existing_table` copies from the `existing_table`:

- `SETTINGS`
- `INNER COLUMNS` for each kind
- `INNER ENGINE` for each kind

The statement is not allowed if the `existing_table` has external targets.
The outer column list is regenerated and not copied.

## Adjusting types of columns {#adjusting-column-types}

You can adjust the types of columns in the inner target tables using the `INNER COLUMNS` clause. For example, to store timestamps in microseconds and values as `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

The same clause can be used to specify codecs and other column attributes:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

## The `id` column {#id-column}

The `id` column contains identifiers, every identifier is calculated for a combination of a metric name and tags.
The type and the `DEFAULT` expression used to generate identifiers can be customized via the `TAGS INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

The `id` column type must be one of `UUID`, `UInt64`, `UInt128`, or `FixedString(16)`. If no `DEFAULT` expression is given, ClickHouse will choose it automatically based on the `id` type. The `id` types declared in the samples and tags inner tables must match.

The `id_generator` setting offers the same customization without using the `INNER COLUMNS` clause:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

If the setting is set, it's used to generate `id` even if the column's `DEFAULT` contains a different expression.

## The `tags` and `all_tags` columns {#tags-and-all-tags}

There are two columns containing maps of tags - `tags` and `all_tags`. In this example they mean the same, however they can be different
if setting `tags_to_columns` is used. This setting allows to specify that a specific tag should be stored in a separate column instead of storing
in a map inside the `tags` column:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

This statement will add columns `instance` and `job` to the inner [tags](#tags-table) target table.
In this case the `tags` column will not contain tags `instance` and `job`,
but the `all_tags` column will contain them. The `all_tags` column is ephemeral and its only purpose to be used in the DEFAULT expression
for the `id` column.

## Table engines of inner target tables {#inner-table-engines}

By default inner target tables use the following table engines:
- the [samples](#samples-table) table uses [MergeTree](../mergetree-family/mergetree);
- the [tags](#tags-table) table uses [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates, and also because it's required to do aggregation for columns `min_time` and `max_time`;
- the [metrics](#metrics-table) table uses [ReplacingMergeTree](../mergetree-family/replacingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates.

Other table engines also can be used for inner target tables if it's specified so:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

## External target tables {#external-target-tables}

It's possible to make a `TimeSeries` table use a manually created table:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

The external tables' column types (`id`, `timestamp`, `value`, and the `<tag_value_column>`s listed in [`tags_to_columns`](#settings)) must match what the `TimeSeries` table would otherwise generate internally (see [Samples table](#samples-table), [Tags table](#tags-table), and [Metrics table](#metrics-table) for the type constraints). Type mismatches are reported at `CREATE` time.

The id-generator expression for an external tags target is resolved at INSERT time in the following order: the [`id_generator`](#settings) setting (if set), then the `DEFAULT` declared on the external table's `id` column (if any), then the canonical generator derived from the `id` type. The setting therefore overrides whatever `DEFAULT` is declared on the external table — see [The `id` column](#id-column) for details.

## Altering settings {#altering-settings}

Three settings can be changed after `CREATE`:

- `id_generator`
- `filter_by_min_time_and_max_time`
- `prometheus_remote_write_dynamic_routing_enabled`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
ALTER TABLE my_table MODIFY SETTING prometheus_remote_write_dynamic_routing_enabled = 1;
```

Note that changing `id_generator` while data is already in the tags table can produce different IDs for the same metric+tag combination — old rows keep their old IDs, new rows use the new generator.

The other settings can't be changed with `ALTER ... MODIFY SETTING` because they are baked into the schema of the inner tables at `CREATE` time.

## Settings {#settings}

Here is a list of settings which can be specified while defining a `TimeSeries` table:

| Name | Type | Default | Description |
|---|---|---|---|
| `id_generator` | Expression | depends on `id` type | Expression that computes the identifier (fingerprint) of a time series from its tags. If unset, the default expression for the `id` column is used. If the default expression for the `id` column is also unset then the expression is chosen automatically |
| `tags_to_columns` | Map | {} | Map specifying which tags should be put to separate columns in the [tags](#tags-table) table. Syntax: `{'tag1': 'column1', 'tag2' : column2, ...}` |
| `use_all_tags_column_to_generate_id` | Bool | true | When generating an expression to calculate an identifier of a time series, this flag enables using the `all_tags` column in that calculation |
| `store_min_time_and_max_time` | Bool | true | If set to true then the table will store `min_time` and `max_time` for each time series |
| `aggregate_min_time_and_max_time` | Bool | true | When creating an inner target `tags` table, this flag enables using `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` instead of just `Nullable(DateTime64(3))` as the type of the `min_time` column, and the same for the `max_time` column |
| `filter_by_min_time_and_max_time` | Bool | true | If set to true then the table will use the `min_time` and `max_time` columns for filtering time series |
| `prometheus_remote_write_dynamic_routing_enabled` | Bool | false | Allow Prometheus remote-write dynamic URL routing to insert into this TimeSeries table |

# Functions {#functions}

Here is a list of functions supporting a `TimeSeries` table as an argument:
- [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
- [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
- [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)
