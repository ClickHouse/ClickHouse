---
slug: /en/operations/system-tables/asynchronous_metrics
---
# asynchronous_metrics

Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

Columns:

- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.
- `description` ([String](../../sql-reference/data-types/string.md) - Metric description)

**Example**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AsynchronousMetricsCalculationTimeSpent │ 0.00179053 │ Time in seconds spent for calculation of asynchronous metrics (this is the overhead of asynchronous metrics).                                                                                                                                              │
│ NumberOfDetachedByUserParts             │          0 │ The total number of parts detached from MergeTree tables by users with the `ALTER TABLE DETACH` query (as opposed to unexpected, broken or ignored parts). The server does not care about detached parts and they can be removed.                          │
│ NumberOfDetachedParts                   │          0 │ The total number of parts detached from MergeTree tables. A part can be detached by a user with the `ALTER TABLE DETACH` query or by the server itself it the part is broken, unexpected or unneeded. The server does not care about detached parts and they can be removed. │
│ TotalRowsOfMergeTreeTables              │    2781309 │ Total amount of rows (records) stored in all tables of MergeTree family.                                                                                                                                                                                   │
│ TotalBytesOfMergeTreeTables             │    7741926 │ Total amount of bytes (compressed, including data and indices) stored in all tables of MergeTree family.                                                                                                                                                   │
│ NumberOfTables                          │         93 │ Total number of tables summed across the databases on the server, excluding the databases that cannot contain MergeTree tables. The excluded database engines are those who generate the set of tables on the fly, like `Lazy`, `MySQL`, `PostgreSQL`, `SQlite`. │
│ NumberOfDatabases                       │          6 │ Total number of databases on the server.                                                                                                                                                                                                                   │
│ MaxPartCountForPartition                │          6 │ Maximum number of parts per partition across all partitions of all tables of MergeTree family. Values larger than 300 indicates misconfiguration, overload, or massive data loading.                                                                       │
│ ReplicasSumMergesInQueue                │          0 │ Sum of merge operations in the queue (still to be applied) across Replicated tables.                                                                                                                                                                       │
│ ReplicasSumInsertsInQueue               │          0 │ Sum of INSERT operations in the queue (still to be replicated) across Replicated tables.                                                                                                                                                                   │
└─────────────────────────────────────────┴────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

<!--- Unlike with system.events and system.metrics, the asynchronous metrics are not gathered in a simple list in a source code file - they
      are mixed with logic in src/Interpreters/ServerAsynchronousMetrics.cpp.
      Listing them here explicitly for reader convenience. --->

## Metric descriptions


### AsynchronousHeavyMetricsCalculationTimeSpent

Time in seconds spent for calculation of asynchronous heavy (tables related) metrics (this is the overhead of asynchronous metrics).

### AsynchronousHeavyMetricsUpdateInterval

Heavy (tables related) metrics update interval

### AsynchronousMetricsCalculationTimeSpent

Time in seconds spent for calculation of asynchronous metrics (this is the overhead of asynchronous metrics).

### AsynchronousMetricsUpdateInterval

Metrics update interval

### BlockActiveTime_*name*

Time in seconds the block device had the IO requests queued. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardBytes_*name*

Number of discarded bytes on the block device. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardMerges_*name*

Number of discard operations requested from the block device and merged together by the OS IO scheduler. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardOps_*name*

Number of discard operations requested from the block device. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardTime_*name*

Time in seconds spend in discard operations requested from the block device, summed across all the operations. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockInFlightOps_*name*

This value counts the number of I/O requests that have been issued to the device driver but have not yet completed. It does not include IO requests that are in the queue but not yet issued to the device driver. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockQueueTime_*name*

This value counts the number of milliseconds that IO requests have waited on this block device. If there are multiple IO requests waiting, this value will increase as the product of the number of milliseconds times the number of requests waiting. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadBytes_*name*

Number of bytes read from the block device. It can be lower than the number of bytes read from the filesystem due to the usage of the OS page cache, that saves IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadMerges_*name*

Number of read operations requested from the block device and merged together by the OS IO scheduler. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadOps_*name*

Number of read operations requested from the block device. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadTime_*name*

Time in seconds spend in read operations requested from the block device, summed across all the operations. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteBytes_*name*

Number of bytes written to the block device. It can be lower than the number of bytes written to the filesystem due to the usage of the OS page cache, that saves IO. A write to the block device may happen later than the corresponding write to the filesystem due to write-through caching. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteMerges_*name*

Number of write operations requested from the block device and merged together by the OS IO scheduler. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteOps_*name*

Number of write operations requested from the block device. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteTime_*name*

Time in seconds spend in write operations requested from the block device, summed across all the operations. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### CPUFrequencyMHz_*name*

The current frequency of the CPU, in MHz. Most of the modern CPUs adjust the frequency dynamically for power saving and Turbo Boosting.

### CompiledExpressionCacheBytes

Total bytes used for the cache of JIT-compiled code.

### CompiledExpressionCacheCount

Total entries in the cache of JIT-compiled code.

### DiskAvailable_*name*

Available bytes on the disk (virtual filesystem). Remote filesystems can show a large value like 16 EiB.

### DiskTotal_*name*

The total size in bytes of the disk (virtual filesystem). Remote filesystems can show a large value like 16 EiB.

### DiskUnreserved_*name*

Available bytes on the disk (virtual filesystem) without the reservations for merges, fetches, and moves. Remote filesystems can show a large value like 16 EiB.

### DiskUsed_*name*

Used bytes on the disk (virtual filesystem). Remote filesystems not always provide this information.

### FilesystemCacheBytes

Total bytes in the `cache` virtual filesystem. This cache is hold on disk.

### FilesystemCacheFiles

Total number of cached file segments in the `cache` virtual filesystem. This cache is hold on disk.

### FilesystemLogsPathAvailableBytes

Available bytes on the volume where ClickHouse logs path is mounted. If this value approaches zero, you should tune the log rotation in the configuration file.

### FilesystemLogsPathAvailableINodes

The number of available inodes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathTotalBytes

The size of the volume where ClickHouse logs path is mounted, in bytes. It's recommended to have at least 10 GB for logs.

### FilesystemLogsPathTotalINodes

The total number of inodes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathUsedBytes

Used bytes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathUsedINodes

The number of used inodes on the volume where ClickHouse logs path is mounted.

### FilesystemMainPathAvailableBytes

Available bytes on the volume where the main ClickHouse path is mounted.

### FilesystemMainPathAvailableINodes

The number of available inodes on the volume where the main ClickHouse path is mounted. If it is close to zero, it indicates a misconfiguration, and you will get 'no space left on device' even when the disk is not full.

### FilesystemMainPathTotalBytes

The size of the volume where the main ClickHouse path is mounted, in bytes.

### FilesystemMainPathTotalINodes

The total number of inodes on the volume where the main ClickHouse path is mounted. If it is less than 25 million, it indicates a misconfiguration.

### FilesystemMainPathUsedBytes

Used bytes on the volume where the main ClickHouse path is mounted.

### FilesystemMainPathUsedINodes

The number of used inodes on the volume where the main ClickHouse path is mounted. This value mostly corresponds to the number of files.

### HTTPThreads

Number of threads in the server of the HTTP interface (without TLS).

### InterserverThreads

Number of threads in the server of the replicas communication protocol (without TLS).

### Jitter

The difference in time the thread for calculation of the asynchronous metrics was scheduled to wake up and the time it was in fact, woken up. A proxy-indicator of overall system latency and responsiveness.

### LoadAverage_*N*

The whole system load, averaged with exponential smoothing over 1 minute. The load represents the number of threads across all the processes (the scheduling entities of the OS kernel), that are currently running by CPU or waiting for IO, or ready to run but not being scheduled at this point of time. This number includes all the processes, not only clickhouse-server. The number can be greater than the number of CPU cores, if the system is overloaded, and many processes are ready to run but waiting for CPU or IO.

### MMapCacheCells

The number of files opened with `mmap` (mapped in memory). This is used for queries with the setting `local_filesystem_read_method` set to  `mmap`. The files opened with `mmap` are kept in the cache to avoid costly TLB flushes.

### MarkCacheBytes

Total size of mark cache in bytes

### MarkCacheFiles

Total number of mark files cached in the mark cache

### MaxPartCountForPartition

Maximum number of parts per partition across all partitions of all tables of MergeTree family. Values larger than 300 indicates misconfiguration, overload, or massive data loading.

### MemoryCode

The amount of virtual memory mapped for the pages of machine code of the server process, in bytes.

### MemoryDataAndStack

The amount of virtual memory mapped for the use of stack and for the allocated memory, in bytes. It is unspecified whether it includes the per-thread stacks and most of the allocated memory, that is allocated with the 'mmap' system call. This metric exists only for completeness reasons. I recommend to use the `MemoryResident` metric for monitoring.

### MemoryResidentMax

Maximum amount of physical memory used by the server process, in bytes.

### MemoryResident

The amount of physical memory used by the server process, in bytes.

### MemoryShared

The amount of memory used by the server process, that is also shared by another processes, in bytes. ClickHouse does not use shared memory, but some memory can be labeled by OS as shared for its own reasons. This metric does not make a lot of sense to watch, and it exists only for completeness reasons.

### MemoryVirtual

The size of the virtual address space allocated by the server process, in bytes. The size of the virtual address space is usually much greater than the physical memory consumption, and should not be used as an estimate for the memory consumption. The large values of this metric are totally normal, and makes only technical sense.

### MySQLThreads

Number of threads in the server of the MySQL compatibility protocol.

### NetworkReceiveBytes_*name*

 Number of bytes received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceiveDrop_*name*

 Number of bytes a packet was dropped while received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceiveErrors_*name*

 Number of times error happened receiving via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceivePackets_*name*

 Number of network packets received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendBytes_*name*

 Number of bytes sent via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendDrop_*name*

 Number of times a packed was dropped while sending via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendErrors_*name*

 Number of times error (e.g. TCP retransmit) happened while sending via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendPackets_*name*

 Number of network packets sent via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NumberOfDatabases

Total number of databases on the server.

### NumberOfDetachedByUserParts

The total number of parts detached from MergeTree tables by users with the `ALTER TABLE DETACH` query (as opposed to unexpected, broken or ignored parts). The server does not care about detached parts, and they can be removed.

### NumberOfDetachedParts

The total number of parts detached from MergeTree tables. A part can be detached by a user with the `ALTER TABLE DETACH` query or by the server itself it the part is broken, unexpected or unneeded. The server does not care about detached parts, and they can be removed.

### NumberOfTables

Total number of tables summed across the databases on the server, excluding the databases that cannot contain MergeTree tables. The excluded database engines are those who generate the set of tables on the fly, like `Lazy`, `MySQL`, `PostgreSQL`, `SQlite`.

### OSContextSwitches

The number of context switches that the system underwent on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSGuestNiceTime

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel, when a guest was set to a higher priority (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestNiceTimeCPU_*N*

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel, when a guest was set to a higher priority (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestNiceTimeNormalized

The value is similar to `OSGuestNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSGuestTime

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestTimeCPU_*N*

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestTimeNormalized

The value is similar to `OSGuestTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSIOWaitTime

The ratio of time the CPU core was not running the code but when the OS kernel did not run any other process on this CPU as the processes were waiting for IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIOWaitTimeCPU_*N*

The ratio of time the CPU core was not running the code but when the OS kernel did not run any other process on this CPU as the processes were waiting for IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIOWaitTimeNormalized

The value is similar to `OSIOWaitTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSIdleTime

The ratio of time the CPU core was idle (not even ready to run a process waiting for IO) from the OS kernel standpoint. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This does not include the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIdleTimeCPU_*N*

The ratio of time the CPU core was idle (not even ready to run a process waiting for IO) from the OS kernel standpoint. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This does not include the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIdleTimeNormalized

The value is similar to `OSIdleTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSInterrupts

The number of interrupts on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSIrqTime

The ratio of time spent for running hardware interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate hardware misconfiguration or a very high network load. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIrqTimeCPU_*N*

The ratio of time spent for running hardware interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate hardware misconfiguration or a very high network load. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIrqTimeNormalized

The value is similar to `OSIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSMemoryAvailable

The amount of memory available to be used by programs, in bytes. This is very similar to the `OSMemoryFreePlusCached` metric. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryBuffers

The amount of memory used by OS kernel buffers, in bytes. This should be typically small, and large values may indicate a misconfiguration of the OS. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryCached

The amount of memory used by the OS page cache, in bytes. Typically, almost all available memory is used by the OS page cache - high values of this metric are normal and expected. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryFreePlusCached

The amount of free memory plus OS page cache memory on the host system, in bytes. This memory is available to be used by programs. The value should be very similar to `OSMemoryAvailable`. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryFreeWithoutCached

The amount of free memory on the host system, in bytes. This does not include the memory used by the OS page cache memory, in bytes. The page cache memory is also available for usage by programs, so the value of this metric can be confusing. See the `OSMemoryAvailable` metric instead. For convenience, we also provide the `OSMemoryFreePlusCached` metric, that should be somewhat similar to OSMemoryAvailable. See also https://www.linuxatemyram.com/. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryTotal

The total amount of memory on the host system, in bytes.

### OSNiceTime

The ratio of time the CPU core was running userspace code with higher priority. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSNiceTimeCPU_*N*

The ratio of time the CPU core was running userspace code with higher priority. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSNiceTimeNormalized

The value is similar to `OSNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSOpenFiles

The total number of opened files on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesBlocked

Number of threads blocked waiting for I/O to complete (`man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesCreated

The number of processes created. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesRunning

The number of runnable (running or ready to run) threads by the operating system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSSoftIrqTime

The ratio of time spent for running software interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate inefficient software running on the system. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSoftIrqTimeCPU_*N*

The ratio of time spent for running software interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate inefficient software running on the system. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSoftIrqTimeNormalized

The value is similar to `OSSoftIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSStealTime

The ratio of time spent in other operating systems by the CPU when running in a virtualized environment. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Not every virtualized environments present this metric, and most of them don't. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSStealTimeCPU_*N*

The ratio of time spent in other operating systems by the CPU when running in a virtualized environment. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Not every virtualized environments present this metric, and most of them don't. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSStealTimeNormalized

The value is similar to `OSStealTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSSystemTime

The ratio of time the CPU core was running OS kernel (system) code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSystemTimeCPU_*N*

The ratio of time the CPU core was running OS kernel (system) code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSystemTimeNormalized

The value is similar to `OSSystemTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSThreadsRunnable

The total number of 'runnable' threads, as the OS kernel scheduler seeing it.

### OSThreadsTotal

The total number of threads, as the OS kernel scheduler seeing it.

### OSUptime

The uptime of the host server (the machine where ClickHouse is running), in seconds.

### OSUserTime

The ratio of time the CPU core was running userspace code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This includes also the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSUserTimeCPU_*N*

The ratio of time the CPU core was running userspace code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This includes also the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSUserTimeNormalized

The value is similar to `OSUserTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### PostgreSQLThreads

Number of threads in the server of the PostgreSQL compatibility protocol.

### QueryCacheBytes

Total size of the query cache in bytes.

### QueryCacheEntries

Total number of entries in the query cache.

### ReplicasMaxAbsoluteDelay

Maximum difference in seconds between the most fresh replicated part and the most fresh data part still to be replicated, across Replicated tables. A very high value indicates a replica with no data.

### ReplicasMaxInsertsInQueue

Maximum number of INSERT operations in the queue (still to be replicated) across Replicated tables.

### ReplicasMaxMergesInQueue

Maximum number of merge operations in the queue (still to be applied) across Replicated tables.

### ReplicasMaxQueueSize

Maximum queue size (in the number of operations like get, merge) across Replicated tables.

### ReplicasMaxRelativeDelay

Maximum difference between the replica delay and the delay of the most up-to-date replica of the same table, across Replicated tables.

### ReplicasSumInsertsInQueue

Sum of INSERT operations in the queue (still to be replicated) across Replicated tables.

### ReplicasSumMergesInQueue

Sum of merge operations in the queue (still to be applied) across Replicated tables.

### ReplicasSumQueueSize

Sum queue size (in the number of operations like get, merge) across Replicated tables.

### TCPThreads

Number of threads in the server of the TCP protocol (without TLS).

### Temperature_*N*

The temperature of the corresponding device in ℃. A sensor can return an unrealistic value. Source: `/sys/class/thermal`

### Temperature_*name*

The temperature reported by the corresponding hardware monitor and the corresponding sensor in ℃. A sensor can return an unrealistic value. Source: `/sys/class/hwmon`

### TotalBytesOfMergeTreeTables

Total amount of bytes (compressed, including data and indices) stored in all tables of MergeTree family.

### TotalPartsOfMergeTreeTables

Total amount of data parts in all tables of MergeTree family. Numbers larger than 10 000 will negatively affect the server startup time, and it may indicate unreasonable choice of the partition key.

### TotalPrimaryKeyBytesInMemory

The total amount of memory (in bytes) used by primary key values (only takes active parts into account).

### TotalPrimaryKeyBytesInMemoryAllocated

The total amount of memory (in bytes) reserved for primary key values (only takes active parts into account).

### TotalRowsOfMergeTreeTables

Total amount of rows (records) stored in all tables of MergeTree family.

### UncompressedCacheBytes

Total size of uncompressed cache in bytes. Uncompressed cache does not usually improve the performance and should be mostly avoided.

### UncompressedCacheCells

Total number of entries in the uncompressed cache. Each entry represents a decompressed block of data. Uncompressed cache does not usually improve performance and should be mostly avoided.

### Uptime

The server uptime in seconds. It includes the time spent for server initialization before accepting connections.

### jemalloc.active

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.allocated

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.dirty_purged

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.muzzy_purged

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pactive

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pdirty

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pmuzzy

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.num_runs

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.num_threads

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.run_intervals

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.epoch

An internal incremental update number of the statistics of jemalloc (Jason Evans' memory allocator), used in all other `jemalloc` metrics.

### jemalloc.mapped

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.metadata

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.metadata_thp

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.resident

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.retained

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.prof.active

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

**See Also**

- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
- [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — Contains instantly calculated metrics.
- [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that have occurred.
- [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
