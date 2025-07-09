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
    <endpoint>/metrics</endpoint>
    <metrics>true</metrics>
    <asynchronous_metrics>true</asynchronous_metrics>
    <events>true</events>
    <errors>true</errors>
</prometheus>

Section `<prometheus.handlers>` can be used to make more extended handlers.
This section is similar to [<http_handlers>](/en/interfaces/http) but works for prometheus protocols:

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/metrics</url>
            <handler>
                <type>expose_metrics</type>
                <metrics>true</metrics>
                <asynchronous_metrics>true</asynchronous_metrics>
                <events>true</events>
                <errors>true</errors>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `port` | none | Port for serving the exposing metrics protocol. |
| `endpoint` | `/metrics` | HTTP endpoint for scraping metrics by prometheus server. Starts with `/`. Should not be used with the `<handlers>` section. |
| `url` / `headers` / `method` | none | Filters used to find a matching handler for a request. Similar to the fields with the same names in the [<http_handlers>](/en/interfaces/http) section. |
| `metrics` | true | Expose metrics from the [system.metrics](/en/operations/system-tables/metrics) table. |
| `asynchronous_metrics` | true | Expose current metrics values from the [system.asynchronous_metrics](/en/operations/system-tables/asynchronous_metrics) table. |
| `events` | true | Expose metrics from the [system.events](/en/operations/system-tables/events) table. |
| `errors` | true | Expose the number of errors by error codes occurred since the last server restart. This information could be obtained from the [system.errors](/en/operations/system-tables/errors) as well. |

Check (replace `127.0.0.1` with the IP addr or hostname of your ClickHouse server):
```bash
curl 127.0.0.1:9363/metrics
```

## Remote-write protocol {#remote-write}

ClickHouse supports the [remote-write](https://prometheus.io/docs/specs/remote_write_spec/) protocol.
Data are received by this protocol and written to a [TimeSeries](/en/engines/table-engines/special/time_series) table
(which should be created beforehand).

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/write</url>
            <handler>
                <type>remote_write</type>
                <database>db_name</database>
                <table>time_series_table</table>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `port` | none | Port for serving the `remote-write` protocol. |
| `url` / `headers` / `method` | none | Filters used to find a matching handler for a request. Similar to the fields with the same names in the [<http_handlers>](/en/interfaces/http) section. |
| `table` | none | The name of a [TimeSeries](/en/engines/table-engines/special/time_series) table to write data received by the `remote-write` protocol. This name can optionally contain the name of a database too. |
| `database` | none | The name of a database where the table specified in the `table` setting is located if it's not specified in the `table` setting. |

## Remote-read protocol {#remote-read}

ClickHouse supports the [remote-read](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/) protocol.
Data are read from a [TimeSeries](/en/engines/table-engines/special/time_series) table and sent via this protocol.

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/read</url>
            <handler>
                <type>remote_read</type>
                <database>db_name</database>
                <table>time_series_table</table>
            </handler>
        </my_rule_1>
    </handlers>
</prometheus>
```

Settings:

| Name | Default | Description |
|---|---|---|---|
| `port` | none | Port for serving the `remote-read` protocol. |
| `url` / `headers` / `method` | none | Filters used to find a matching handler for a request. Similar to the fields with the same names in the [<http_handlers>](/en/interfaces/http) section. |
| `table` | none | The name of a [TimeSeries](/en/engines/table-engines/special/time_series) table to read data to send by the `remote-read` protocol. This name can optionally contain the name of a database too. |
| `database` | none | The name of a database where the table specified in the `table` setting is located if it's not specified in the `table` setting. |

## Configuration for multiple protocols {#multiple-protocols}

Multiple protocols can be specified together in one place:

```xml
<prometheus>
    <port>9363</port>
    <handlers>
        <my_rule_1>
            <url>/metrics</url>
            <handler>
                <type>expose_metrics</type>
                <metrics>true</metrics>
                <asynchronous_metrics>true</asynchronous_metrics>
                <events>true</events>
                <errors>true</errors>
            </handler>
        </my_rule_1>
        <my_rule_2>
            <url>/write</url>
            <handler>
                <type>remote_write</type>
                <table>db_name.time_series_table</table>
            </handler>
        </my_rule_2>
        <my_rule_3>
            <url>/read</url>
            <handler>
                <type>remote_read</type>
                <table>db_name.time_series_table</table>
            </handler>
        </my_rule_3>
    </handlers>
</prometheus>
```
