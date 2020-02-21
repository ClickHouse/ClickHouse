# Monitoring

You can monitor:

- Utilization of hardware resources.
- ClickHouse server metrics.

## Resource Utilization

ClickHouse does not monitor the state of hardware resources by itself.

It is highly recommended to set up monitoring for:

- Load and temperature on processors.

    You can use [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) or other instruments.

- Utilization of storage system, RAM and network.

## ClickHouse Server Metrics

ClickHouse server has embedded instruments for self-state monitoring.

To track server events use server logs. See the [logger](server_settings/settings.md#server_settings-logger) section of the configuration file.

ClickHouse collects:

- Different metrics of how the server uses computational resources.
- Common statistics on query processing.

You can find metrics in the [system.metrics](system_tables.md#system_tables-metrics), [system.events](system_tables.md#system_tables-events), and [system.asynchronous_metrics](system_tables.md#system_tables-asynchronous_metrics) tables.

You can configure ClickHouse to export metrics to [Graphite](https://github.com/graphite-project). See the [Graphite section](server_settings/settings.md#server_settings-graphite) in the ClickHouse server configuration file. Before configuring export of metrics, you should set up Graphite by following their official [guide](https://graphite.readthedocs.io/en/latest/install.html).

Additionally, you can monitor server availability through the HTTP API. Send the `HTTP GET` request to `/`. If the server is available, it responds with `200 OK`.

To monitor servers in a cluster configuration, you should set the [max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) parameter and use the HTTP resource `/replicas_status`. A request to `/replicas_status` returns `200 OK` if the replica is available and is not delayed behind the other replicas. If a replica is delayed, it returns `503 HTTP_SERVICE_UNAVAILABLE` with information about the gap.
