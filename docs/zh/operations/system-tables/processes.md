# system.processes {#system_tables-processes}

该系统表用于实现 `SHOW PROCESSLIST` 查询。

列:

-   `user` (String) – 进行查询的用户。请记住，对于分布式处理，查询将以 `default` 用户发送到远程服务器。 该字段包含特定查询的用户名，而不是此查询初始的用户名。
-   `address` (String) – 发出请求的IP地址。分布式处理也是如此。若要跟踪分布式查询最初来自何处，请在请求者服务器上查看 `system.processes` 。
-   `elapsed` (Float64) – 请求执行的时间（以秒为单位）。
-   `rows_read` (UInt64) – 从表中读取的行数。对于分布式处理，在请求者服务器上，这是所有远程服务器的总数。
-   `bytes_read` (UInt64) – 从表中读取的未压缩字节数。对于分布式处理，在请求者服务器上，这是所有远程服务器的总数。
-   `total_rows_approx` (UInt64) – 应该读取的总行数的近似值。对于分布式处理，在请求者服务器上，这是所有远程服务器的总数。当已知要处理的新资源时，可以在请求处理期间对其进行更新。
-   `memory_usage` (UInt64) – 请求使用的RAM量。它可能不包括某些类型的专用内存。请参阅 [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage) 设置。
-   `query` (String) – 查询文本。对于 `INSERT`，它不包括要插入的数据。
-   `query_id` (String) – 查询ID（如果已定义）。

```sql
:) SELECT * FROM system.processes LIMIT 10 FORMAT Vertical;
```

```text
Row 1:
──────
is_initial_query:     1
user:                 default
query_id:             35a360fa-3743-441d-8e1f-228c938268da
address:              ::ffff:172.23.0.1
port:                 47588
initial_user:         default
initial_query_id:     35a360fa-3743-441d-8e1f-228c938268da
initial_address:      ::ffff:172.23.0.1
initial_port:         47588
interface:            1
os_user:              bharatnc
client_hostname:      tower
client_name:          ClickHouse 
client_revision:      54437
client_version_major: 20
client_version_minor: 7
client_version_patch: 2
http_method:          0
http_user_agent:      
quota_key:            
elapsed:              0.000582537
is_cancelled:         0
read_rows:            0
read_bytes:           0
total_rows_approx:    0
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
query:                SELECT * from system.processes LIMIT 10 FORMAT Vertical;
thread_ids:           [67]
ProfileEvents.Names:  ['Query','SelectQuery','ReadCompressedBytes','CompressedReadBufferBlocks','CompressedReadBufferBytes','IOBufferAllocs','IOBufferAllocBytes','ContextLock','RWLockAcquiredReadLocks']
ProfileEvents.Values: [1,1,36,1,10,1,89,16,1]
Settings.Names:       ['use_uncompressed_cache','load_balancing','log_queries','max_memory_usage']
Settings.Values:      ['0','in_order','1','10000000000']

1 rows in set. Elapsed: 0.002 sec. 
```

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/processes) <!--hide-->
