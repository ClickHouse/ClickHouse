---
toc_priority: 45
toc_title: "监控"
---

# 监控 {#jian-kong}

可以监控到：

-   硬件资源的利用率。
-   ClickHouse 服务的指标。

## 硬件资源利用率 {#ying-jian-zi-yuan-li-yong-lu}

ClickHouse 本身不会去监控硬件资源的状态。

强烈推荐监控以下监控项：

-   处理器上的负载和温度。

       可以使用[dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html)或者其他工具。

-   磁盘存储，RAM和网络的使用率。

## ClickHouse 服务的指标。 {#clickhouse-fu-wu-de-zhi-biao}

ClickHouse服务本身具有用于自我状态监视指标。

要跟踪服务器事件，请观察服务器日志。 请参阅配置文件的 [logger](server-configuration-parameters/settings.md#server_configuration_parameters-logger)部分。

ClickHouse 收集的指标项：

-   服务用于计算的资源占用的各种指标。
-   关于查询处理的常见统计信息。

可以在[系统指标](system-tables/metrics.md#system_tables-metrics)，[系统事件](system-tables/events.md#system_tables-events)以及[系统异步指标](system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics)等系统表查看所有的指标项。

可以配置ClickHouse向[Graphite](https://github.com/graphite-project)推送监控信息并导入指标。参考[Graphite监控](server-configuration-parameters/settings.md#server_configuration_parameters-graphite)配置文件。在配置指标导出之前，需要参考[Graphite官方教程](https://graphite.readthedocs.io/en/latest/install.html)搭建Graphite服务。

此外，您可以通过HTTP API监视服务器可用性。将HTTP GET请求发送到`/ping`。如果服务器可用，它将以 `200 OK` 响应。

要监视服务器集群的配置，应设置[max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries)参数并使用HTTP资源`/replicas_status`。 如果副本可用，并且不延迟在其他副本之后，则对`/replicas_status`的请求将返回`200 OK`。 如果副本滞后，请求将返回`503 HTTP_SERVICE_UNAVAILABLE`，包括有关待办事项大小的信息。
