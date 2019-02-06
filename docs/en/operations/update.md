# ClickHouse Update

Update of ClickHouse should be performed with the server shutdown.

If ClickHouse is installed from deb-packages, execute the following commands on the server:

```
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

For other methods of ClickHouse installation use corresponding methods of update.

ClickHouse does not support a distributed update. The operation should be performed at each separate server.
