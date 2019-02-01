# Monitoring

You can monitor:

- Hardware server.
- ClickHouse server.

## Hardware server

ClickHouse does not monitor the state of hardware server resources by itself.

It is highly recommended to set up monitoring for:

- Processors load and temperature.

    You can use `dmesg`, `turbostat` or other instruments.

- Utilization of storage system, RAM and network.

## ClickHouse server

ClickHouse server has embedded instruments for monitoring of self-state.

To monitor server events use server logs. See the [logger](#server_settings-logger) section of the configuration file.

ClickHouse collects different metrics of computational resources usage and common statistics of queries processing. You can find metrics in tables `system.metrics`, `system.events` и `system.asynchronous_metrics`.

You can configure ClickHouse to export metrics to [Graphite](https://github.com/graphite-project). See the [Graphite section](server_settings/settings.md#server_settings-graphite) of ClickHouse server configuration file. You should install Graphite by yourself.

Also, you can monitor server availability through the HTTP API. Send the `HTTP GET` request to `/`. If server available, it answers `200 OK` or nothing otherwise.

To monitor servers in a cluster configuration, you should set [max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) parameter and use HTTP resource `/replicas-delay`. Request to `/replicas-delay` returns `200 OK` if the replica is available and does not delay behind others. If replica delays, it returns the information about the gap.
