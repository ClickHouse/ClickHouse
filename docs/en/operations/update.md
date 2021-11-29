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

!!! note "Note"
    You can update multiple servers at once as soon as there is no moment when all replicas of one shard are offline.    

The upgrade of older version of ClickHouse to specific version:

As an example:
 
`xx.yy.a.b` is a current stable version. The latest stable version could be found [here](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```
