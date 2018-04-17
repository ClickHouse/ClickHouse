# system.clusters

Contains information about clusters available in the config file and the servers in them.
Columns:

```text
cluster String      - Cluster name.
shard_num UInt32    - Number of a shard in the cluster, starting from 1.
shard_weight UInt32 - Relative weight of a shard when writing data.
replica_num UInt32  - Number of a replica in the shard, starting from 1.
host_name String    - Host name as specified in the config.
host_address String - Host's IP address obtained from DNS.
port UInt16         - The port used to access the server.
user String         - The username to use for connecting to the server.
```
