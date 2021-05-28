---
toc_priority: 47
toc_title: ClickHouse Update
---

# ClickHouse Update {#clickhouse-update}

If ClickHouse was installed from `deb` packages, execute the following commands on the server:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

If you installed ClickHouse using something other than the recommended `deb` packages, use the appropriate update method.

ClickHouse does not support a distributed update. The operation should be performed consecutively on each separate server. Do not update all the servers on a cluster simultaneously, or the cluster will be unavailable for some time.
