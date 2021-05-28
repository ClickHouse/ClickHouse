---
toc_priority: 14
toc_title: "\u266A\u64CD\u573A\u266A"
---

# ClickHouse体验平台 {#clickhouse-playground}

[ClickHouse体验平台](https://play.clickhouse.tech?file=welcome) 允许人们通过即时运行查询来尝试ClickHouse，而无需设置他们的服务器或集群。
体验平台中提供了几个示例数据集以及显示ClickHouse特性的示例查询。

查询以只读用户身份执行。 这意味着一些局限性:

-   不允许DDL查询
-   不允许插入查询

还强制执行以下设置:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouse体验还有如下：
[ClickHouse管理服务](https://cloud.yandex.com/services/managed-clickhouse)
实例托管 [Yandex云](https://cloud.yandex.com/).
更多信息 [云提供商](../commercial/cloud.md).

ClickHouse体验平台界面实际上是通过ClickHouse [HTTP API](../interfaces/http.md)接口实现的.
体验平台后端只是一个ClickHouse集群，没有任何额外的服务器端应用程序。
体验平台也同样提供了ClickHouse HTTPS服务端口。

您可以使用任何HTTP客户端向体验平台进行查询，例如 [curl](https://curl.haxx.se) 或 [wget](https://www.gnu.org/software/wget/)，或使用以下方式建立连接 [JDBC](../interfaces/jdbc.md) 或 [ODBC](../interfaces/odbc.md) 司机
有关支持ClickHouse的软件产品的更多信息，请访问 [这里](../interfaces/index.md).

| 参数     | 值                                    |
|:---------|:--------------------------------------|
| 服务端口 | https://play-api.clickhouse.tech:8443 |
| 用户     | `playground`                          |
| 密码     | `clickhouse`                          |

请注意，此服务端口需要安全连接。

示例:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
