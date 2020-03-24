# ClickHouse Playground {#clickhouse-playground}

ClickHouse Playground allows users to run ClickHouse queries instantly.
Several example datasets are available in Playground as well as sample queries that show ClickHouse features.

The queries are run as readonly user. This implies some limitations:
- DDL queries are not allowed
- INSERT queries are not allowed

ClickHouse Playground gives the experience of m2.small
[Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
instance hosted in [Yandex.Cloud](https://cloud.yandex.com/).
More information about [cloud providers](../commercial/cloud.md).

ClickHouse Playground web interface makes requests via ClickHouse HTTP API.
Backend is just a ClickHouse cluster.
ClickHouse HTTP endpoint is is also available as a part of Playground.

You can make queries to playground using curl/wget or set up a connection using JDBC/ODBC drivers.
More information about software products that support ClickHouse is available [here](../interfaces/index.md).

| Parameter | Value                                 |
|:----------|:--------------------------------------|
| Endpoint  | https://play-api.clickhouse.tech:8443 |
| User      | `playground`                          |
| Password  | `clickhouse`                          |

Note that endpoint requires a secure connection.

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
