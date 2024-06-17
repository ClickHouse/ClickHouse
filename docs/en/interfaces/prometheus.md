---
slug: /en/interfaces/prometheus
sidebar_position: 19
sidebar_label: Prometheus protocols
---

# Prometheus protocols

## Exposing metrics {#expose}

:::note
ClickHouse Cloud does not currently support connecting to Prometheus. To be notified when this feature is supported, please contact support@clickhouse.com.
:::

ClickHouse can expose its own metrics for scraping from Prometheus:

```xml
<prometheus>
    <port>9363</port>
    <expose>
        <endpoint>/metrics</endpoint>
        <metrics>true</metrics>
        <asynchronous_metrics>true</asynchronous_metrics>
        <events>true</events>
        <errors>true</errors>
    </expose>
</prometheus>
```

An alternative format without the `<expose>` tag is also supported:
```xml
<prometheus>
    <port>9363</port>
    <endpoint>/metrics</endpoint>
    <metrics>true</metrics>
    <asynchronous_metrics>true</asynchronous_metrics>
    <events>true</events>
    <errors>true</errors>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `endpoint` | `/metrics` | HTTP endpoint for scraping metrics by prometheus server. Starts with `/`. |
| `port` | none | Port for `endpoint` |
| `metrics` | true | Expose metrics from the [system.metrics](../operations/system-tables/metrics.md#system_tables-metrics) table. |
| `asynchronous_metrics` | true | Expose current metrics values from the [system.asynchronous_metrics](../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table. |
| `events` | true | Expose metrics from the [system.events](../operations/system-tables/events.md#system_tables-events) table. |
| `errors` | true | Expose the number of errors by error codes occurred since the last server restart. This information could be obtained from the [system.errors](../operations/system-tables/asynchronous_metrics.md#system_tables-errors) as well. |

Check (replace `127.0.0.1` with the IP addr or hostname of your ClickHouse server):
```bash
curl 127.0.0.1:9363/metrics
```

## Remote-write protocol {#remote-write}

ClickHouse supports the [remote-write](https://prometheus.io/docs/specs/remote_write_spec/) protocol.
Data are received by this protocol and written to a [TimeSeries](../engines/table-engines/integrations/time-series.md) table
(which should be created beforehand).

```xml
<prometheus>
    <port>9363</port>
    <remote_write>
        <endpoint>/write</endpoint>
        <database>db_name</database>
        <table>time_series_table</table>
    </expose>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `endpoint` | `/write` | HTTP endpoint for handling the `remote-write` protocol. Starts with `/`. |
| `port` | none | Port for `endpoint` |
| `table` | none | The name of a [TimeSeries](../engines/table-engines/integrations/time-series.md) table to write data received by the `remote-write` protocol. This name can optionally contain the name of a database too. |
| `database` | none | The name of a database where the table specified in the `table` setting is located if it's not specified in the `table` setting. |

## Remote-read protocol {#remote-read}

ClickHouse supports the [remote-read](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/) protocol.
Data are read from a [TimeSeries](../engines/table-engines/integrations/time-series.md) table and sent via this protocol.

```xml
<prometheus>
    <port>9363</port>
    <remote_read>
        <endpoint>/read</endpoint>
        <database>db_name</database>
        <table>time_series_table</table>
    </expose>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `endpoint` | `/read` | HTTP endpoint for handling the `remote-read` protocol. Starts with `/`. |
| `port` | none | Port for `endpoint` |
| `table` | none | The name of a [TimeSeries](../engines/table-engines/integrations/time-series.md) table to read data to send by the `remote-read` protocol. This name can optionally contain the name of a database too. |
| `database` | none | The name of a database where the table specified in the `table` setting is located if it's not specified in the `table` setting. |

## Configuration for multiple protocols {#multiple-protocols}

Multiple protocols can be specified together in one place:

```xml
<prometheus>
    <port>9363</port>
    <expose>
        <endpoint>/metrics</endpoint>
        <metrics>true</metrics>
        <asynchronous_metrics>true</asynchronous_metrics>
        <events>true</events>
        <errors>true</errors>
    </expose>
    <remote_write>
        <endpoint>/write</endpoint>
        <table>db_name.time_series_table</table>
    </expose>
    <remote_read>
        <endpoint>/read</endpoint>
        <table>db_name.time_series_table</table>
    </expose>
</prometheus>
```
