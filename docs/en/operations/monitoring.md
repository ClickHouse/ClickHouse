# Monitoring

You can monitor:

- Hardware resources utilization.
- ClickHouse server metrics.

## Resources Utilization

ClickHouse does not monitor the state of hardware resources by itself.

It is highly recommended to set up monitoring for:

- Processors load and temperature.

    You can use [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) or other instruments.

- Utilization of storage system, RAM and network.

## ClickHouse Server Metrics

ClickHouse server has embedded instruments for self-state monitoring.

To track server events use server logs. See the [logger](#server_settings-logger) section of the configuration file.

ClickHouse collects:

- Different metrics of how the server uses computational resources.
- Common statistics of queries processing.

You can find metrics in tables [system.metrics](#system_tables-metrics), [system.events](#system_tables-events) и [system.asynchronous_metrics](#system_tables-asynchronous_metrics).

You can configure ClickHouse to export metrics to [Graphite](https://github.com/graphite-project). See the [Graphite section](server_settings/settings.md#server_settings-graphite) of ClickHouse server configuration file. Before configuring metrics export, you should set up Graphite by following their official guide https://graphite.readthedocs.io/en/latest/install.html.

Also, you can monitor server availability through the HTTP API. Send the `HTTP GET` request to `/`. If server available, it answers `200 OK`.

To monitor servers in a cluster configuration, you should set [max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) parameter and use HTTP resource `/replicas-delay`. Request to `/replicas-delay` returns `200 OK` if the replica is available and does not delay behind others. If replica delays, it returns the information about the gap.
