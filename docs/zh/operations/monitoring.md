# 监控

可以监控到：

- 硬件资源的利用率。
- ClickHouse 服务的指标。

## 硬件资源利用率

ClickHouse 本身不会去监控硬件资源的状态。

强烈推荐监控以下监控项：

- 处理器上的负载和温度。

    可以使用 [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) 或者其他工具。

- 磁盘存储，RAM和网络的使用率。

##  ClickHouse 服务的指标。

ClickHouse服务本身具有用于自我状态监视指标。

要跟踪服务器事件，请观察服务器日志。 请参阅配置文件的[logger]（server_settings/settings.md#server_settings-logger）部分。

ClickHouse 收集的指标项：

- 服务用于计算的资源占用的各种指标。
- 关于查询处理的常见统计信息。

可以在 [system.metrics](system_tables.md#system_tables-metrics) ，[system.events](system_tables.md#system_tables-events) 以及[system.asynchronous_metrics](system_tables.md#system_tables-asynchronous_metrics) 等系统表查看所有的指标项。

可以配置ClickHouse 往 [Graphite](https://github.com/graphite-project)导入指标。 参考 [Graphite section](server_settings/settings.md#server_settings-graphite) 配置文件。在配置指标导出之前，需要参考Graphite[官方教程](https://graphite.readthedocs.io/en/latest/install.html)搭建服务。

此外，您可以通过HTTP API监视服务器可用性。 将HTTP GET请求发送到 `/`。 如果服务器可用，它将以 `200 OK` 响应。

要监视服务器集群的配置中，应设置[max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries)参数并使用HTTP资源`/replicas_status`。 如果副本可用，并且不延迟在其他副本之后，则对`/replicas_status`的请求将返回200 OK。 如果副本滞后，请求将返回 `503 HTTP_SERVICE_UNAVAILABLE`，包括有关待办事项大小的信息。
