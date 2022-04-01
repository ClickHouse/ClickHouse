---
toc_priority: 14
toc_title: Playground
---

# ClickHouse Playground {#clickhouse-playground}

!!! warning "Warning"
    This service is deprecated and will be replaced in foreseeable future.

[ClickHouse Playground](https://play.clickhouse.com) allows people to experiment with ClickHouse by running queries instantly, without setting up their server or cluster.
Several example datasets are available in Playground as well as sample queries that show ClickHouse features. There’s also a selection of ClickHouse LTS releases to experiment with.

You can make queries to Playground using any HTTP client, for example [curl](https://curl.haxx.se) or [wget](https://www.gnu.org/software/wget/), or set up a connection using [JDBC](../interfaces/jdbc.md) or [ODBC](../interfaces/odbc.md) drivers. More information about software products that support ClickHouse is available [here](../interfaces/index.md).

## Credentials {#credentials}

| Parameter           | Value                                   |
|:--------------------|:----------------------------------------|
| HTTPS endpoint      | `https://play-api.clickhouse.com:8443` |
| Native TCP endpoint | `play-api.clickhouse.com:9440`         |
| User                | `playground`                            |
| Password            | `clickhouse`                            |

There are additional endpoints with specific ClickHouse releases to experiment with their differences (ports and user/password are the same as above):

-   20.3 LTS: `play-api-v20-3.clickhouse.com`
-   19.14 LTS: `play-api-v19-14.clickhouse.com`

!!! note "Note"
    All these endpoints require a secure TLS connection.

## Limitations {#limitations}

The queries are executed as a read-only user. It implies some limitations:

-   DDL queries are not allowed
-   INSERT queries are not allowed

The following settings are also enforced:

- [max_result_bytes=10485760](../operations/settings/query-complexity/#max-result-bytes)
- [max_result_rows=2000](../operations/settings/query-complexity/#setting-max_result_rows)
- [result_overflow_mode=break](../operations/settings/query-complexity/#result-overflow-mode)
- [max_execution_time=60000](../operations/settings/query-complexity/#max-execution-time)

## Examples {#examples}

HTTPS endpoint example with `curl`:

``` bash
curl "https://play-api.clickhouse.com:8443/?query=SELECT+'Play+ClickHouse\!';&user=playground&password=clickhouse&database=datasets"
```

TCP endpoint example with [CLI](../interfaces/cli.md):

``` bash
clickhouse client --secure -h play-api.clickhouse.com --port 9440 -u playground --password clickhouse -q "SELECT 'Play ClickHouse\!'"
```
