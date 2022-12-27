---
toc_priority: 14
toc_title: 体验平台
---

# ClickHouse体验平台 {#clickhouse-playground}

!!! warning "Warning"
    This service is deprecated and will be replaced in foreseeable future.

[ClickHouse体验平台](https://play.clickhouse.com?file=welcome) 允许人们通过即时运行查询来尝试ClickHouse，而无需设置他们的服务器或集群。

体验平台中提供几个示例数据集以及显示ClickHouse特性的示例查询。还有一些ClickHouse LTS版本可供尝试。

您可以使用任何HTTP客户端对ClickHouse体验平台进行查询，例如[curl](https://curl.haxx.se)或者[wget](https://www.gnu.org/software/wget/),或使用[JDBC](../interfaces/jdbc.md)或者[ODBC](../interfaces/odbc.md)驱动连接。关于支持ClickHouse的软件产品的更多信息详见[here](../interfaces/index.md).

## Credentials {#credentials}

| 参数                 | 值                                      |
|:--------------------|:----------------------------------------|
| HTTPS端点           | `https://play-api.clickhouse.com:8443` |
| TCP端点             | `play-api.clickhouse.com:9440`         |
| 用户                | `playground`                            |
| 密码                | `clickhouse`                            |

还有一些带有特定ClickHouse版本的附加信息来试验它们之间的差异(端口和用户/密码与上面相同):

-   20.3 LTS: `play-api-v20-3.clickhouse.com`
-   19.14 LTS: `play-api-v19-14.clickhouse.com`

!!! note "注意"
    所有这些端点都需要安全的TLS连接。

## 查询限制 {#limitations}

查询以只读用户身份执行。 这意味着一些局限性:

-   不允许DDL查询
-   不允许插入查询

还强制执行以下设置:
- [max_result_bytes=10485760](../operations/settings/query-complexity/#max-result-bytes)
- [max_result_rows=2000](../operations/settings/query-complexity/#setting-max_result_rows)
- [result_overflow_mode=break](../operations/settings/query-complexity/#result-overflow-mode)
- [max_execution_time=60000](../operations/settings/query-complexity/#max-execution-time)

ClickHouse体验还有如下：
[ClickHouse管理服务](https://cloud.yandex.com/services/managed-clickhouse)
实例托管 [Yandex云](https://cloud.yandex.com/)。
更多信息 [云提供商](../commercial/cloud.md)。

## 示例 {#examples}

使用`curl`连接Https服务：

``` bash
curl "https://play-api.clickhouse.com:8443/?query=SELECT+'Play+ClickHouse\!';&user=playground&password=clickhouse&database=datasets"
```

TCP连接示例[CLI](../interfaces/cli.md):

``` bash
clickhouse client --secure -h play-api.clickhouse.com --port 9440 -u playground --password clickhouse -q "SELECT 'Play ClickHouse\!'"
```
