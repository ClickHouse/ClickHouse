---
toc_priority: 47
toc_title: ClickHouse Upgrade
---

# ClickHouse Upgrade {#clickhouse-upgrade}

If ClickHouse was installed from `deb` packages, execute the following commands on the server:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

If you installed ClickHouse using something other than the recommended `deb` packages, use the appropriate update method.

ClickHouse does not support a distributed update. The operation should be performed consecutively on each separate server. Do not update all the servers on a cluster simultaneously, or the cluster will be unavailable for some time.

The upgrade of older version of ClickHouse to specific version:

As an example: 

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=20.12.4.5 clickhouse-client=20.12.4.5 clickhouse-common-static=20.12.4.5
$ sudo service clickhouse-server restart
```

Note: It's always recommended to backup all databases before initiating the upgrade process. Please make sure the new version is compatible with new changes so on.
