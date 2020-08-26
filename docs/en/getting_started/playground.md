---
toc_priority: 14
toc_title: Playground
---

# ClickHouse Playground {#clickhouse-playground}

[ClickHouse Playground](https://play.clickhouse.tech?file=welcome) allows people to experiment with ClickHouse by running queries instantly, without setting up their server or cluster.
Several example datasets are available in the Playground as well as sample queries that show ClickHouse features.

The queries are executed as a read-only user. It implies some limitations:

-   DDL queries are not allowed
-   INSERT queries are not allowed

The following settings are also enforced:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouse Playground gives the experience of m2.small
[Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
instance hosted in [Yandex.Cloud](https://cloud.yandex.com/).
More information about [cloud providers](../commercial/cloud.md).

ClickHouse Playground web interface makes requests via ClickHouse [HTTP API](../interfaces/http.md).
The Playground backend is just a ClickHouse cluster without any additional server-side application.
ClickHouse HTTPS endpoint is also available as a part of the Playground.

You can make queries to playground using any HTTP client, for example [curl](https://curl.haxx.se) or [wget](https://www.gnu.org/software/wget/), or set up a connection using [JDBC](../interfaces/jdbc.md) or [ODBC](../interfaces/odbc.md) drivers.
More information about software products that support ClickHouse is available [here](../interfaces/index.md).

| Parameter | Value                                 |
|:----------|:--------------------------------------|
| Endpoint  | https://play-api.clickhouse.tech:8443 |
| User      | `playground`                          |
| Password  | `clickhouse`                          |

Note that this endpoint requires a secure connection.

Example:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
