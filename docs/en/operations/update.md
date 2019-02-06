# ClickHouse Update

If ClickHouse is installed from deb-packages, execute the following commands on the server:

```
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
sudo service clickhouse-server restart
```

If you installed ClickHouse not from recommended deb-packages, use corresponding methods of update.

ClickHouse does not support a distributed update. The operation should be performed consecutively at each separate server. Do not update all the servers on cluster at the same time, otherwise cluster became unavailable for some time.
