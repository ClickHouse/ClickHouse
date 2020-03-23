# ClickHouse Playground

ClickHouse Playground allows users to run ClickHouse queries instantly, without setting up their own server or cluster.
Several example datasets are available in Playground as well as sample queries that show ClickHouse features.

The queries are run as readonly user. This implies some limitations:

- DDL queries are not allowed
- INSERT queries are not allowed

Following settings are also set:
- `max_result_bytes=10485760` 
- `max_result_rows=2000`
- `result_overflow_mode=break`
- `max_execution_time=60000`

ClickHouse Playground gives the experience of m2.small
[Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
instance hosted in [Yandex.Cloud](https://cloud.yandex.com/).
More information about [cloud providers](../commercial/cloud.md).

ClickHouse Playground web interface makes requests via ClickHouse [HTTP API](../interfaces/http.md).
Backend is just a ClickHouse cluster.
ClickHouse HTTP endpoint is is also available as a part of Playground.

You can make queries to playground using any HTTP client, for example [curl](https://curl.haxx.se) or [wget](https://www.gnu.org/software/wget/), or set up a connection using [JDBC](../interfaces/jdbc.md) or [ODBC](../interfaces/odbc.md) drivers.
More information about software products that support ClickHouse is available [here](../interfaces/index.md).

| Parameter | Value |  
|:----------|:-------------|
| Endpoint| https://play-api.clickhouse.tech:8443 |
| User  | `playground`  |
| Password  | `clickhouse`  |

Note that this endpoint requires a secure connection.

Example:

```bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
