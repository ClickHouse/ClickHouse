---
description: 'System table containing metrics that are calculated periodically in
  the background. For example, the amount of RAM in use.'
keywords: ['system table', 'asynchronous_metrics']
slug: /operations/system-tables/asynchronous_metrics
title: 'system.asynchronous_metrics'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.asynchronous_metrics

<SystemTableCloud/>

Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

Columns:

- `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.
- `description` ([String](../../sql-reference/data-types/string.md) - Metric description)

**Example**

```sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

```text
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

## Metric descriptions {#metric-descriptions}


### AsynchronousHeavyMetricsCalculationTimeSpent {#asynchronousheavymetricscalculationtimespent}

Time in seconds spent for calculation of asynchronous heavy (tables related) metrics (this is the overhead of asynchronous metrics).

### AsynchronousHeavyMetricsUpdateInterval {#asynchronousheavymetricsupdateinterval}

Heavy (tables related) metrics update interval

### AsynchronousMetricsCalculationTimeSpent {#asynchronousmetricscalculationtimespent}

Time in seconds spent for calculation of asynchronous metrics (this is the overhead of asynchronous metrics).

### AsynchronousMetricsUpdateInterval {#asynchronousmetricsupdateinterval}

Metrics update interval

### BlockActiveTime_*name* {#blockactivetime_name}

Time in seconds the block device had the IO requests queued. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardBytes_*name* {#blockdiscardbytes_name}

Number of discarded bytes on the block device. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardMerges_*name* {#blockdiscardmerges_name}

Number of discard operations requested from the block device and merged together by the OS IO scheduler. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardOps_*name* {#blockdiscardops_name}

Number of discard operations requested from the block device. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockDiscardTime_*name* {#blockdiscardtime_name}

Time in seconds spend in discard operations requested from the block device, summed across all the operations. These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockInFlightOps_*name* {#blockinflightops_name}

This value counts the number of I/O requests that have been issued to the device driver but have not yet completed. It does not include IO requests that are in the queue but not yet issued to the device driver. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockQueueTime_*name* {#blockqueuetime_name}

This value counts the number of milliseconds that IO requests have waited on this block device. If there are multiple IO requests waiting, this value will increase as the product of the number of milliseconds times the number of requests waiting. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadBytes_*name* {#blockreadbytes_name}

Number of bytes read from the block device. It can be lower than the number of bytes read from the filesystem due to the usage of the OS page cache, that saves IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadMerges_*name* {#blockreadmerges_name}

Number of read operations requested from the block device and merged together by the OS IO scheduler. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadOps_*name* {#blockreadops_name}

Number of read operations requested from the block device. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockReadTime_*name* {#blockreadtime_name}

Time in seconds spend in read operations requested from the block device, summed across all the operations. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteBytes_*name* {#blockwritebytes_name}

Number of bytes written to the block device. It can be lower than the number of bytes written to the filesystem due to the usage of the OS page cache, that saves IO. A write to the block device may happen later than the corresponding write to the filesystem due to write-through caching. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteMerges_*name* {#blockwritemerges_name}

Number of write operations requested from the block device and merged together by the OS IO scheduler. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteOps_*name* {#blockwriteops_name}

Number of write operations requested from the block device. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### BlockWriteTime_*name* {#blockwritetime_name}

Time in seconds spend in write operations requested from the block device, summed across all the operations. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt

### CPUFrequencyMHz_*name* {#cpufrequencymhz_name}

The current frequency of the CPU, in MHz. Most of the modern CPUs adjust the frequency dynamically for power saving and Turbo Boosting.

### CompiledExpressionCacheBytes {#compiledexpressioncachebytes}

Total bytes used for the cache of JIT-compiled code.

### CompiledExpressionCacheCount {#compiledexpressioncachecount}

Total entries in the cache of JIT-compiled code.

### DiskAvailable_*name* {#diskavailable_name}

Available bytes on the disk (virtual filesystem). Remote filesystems can show a large value like 16 EiB.

### DiskTotal_*name* {#disktotal_name}

The total size in bytes of the disk (virtual filesystem). Remote filesystems can show a large value like 16 EiB.

### DiskUnreserved_*name* {#diskunreserved_name}

Available bytes on the disk (virtual filesystem) without the reservations for merges, fetches, and moves. Remote filesystems can show a large value like 16 EiB.

### DiskUsed_*name* {#diskused_name}

Used bytes on the disk (virtual filesystem). Remote filesystems not always provide this information.

### FilesystemCacheBytes {#filesystemcachebytes}

Total bytes in the `cache` virtual filesystem. This cache is hold on disk.

### FilesystemCacheFiles {#filesystemcachefiles}

Total number of cached file segments in the `cache` virtual filesystem. This cache is hold on disk.

### FilesystemLogsPathAvailableBytes {#filesystemlogspathavailablebytes}

Available bytes on the volume where ClickHouse logs path is mounted. If this value approaches zero, you should tune the log rotation in the configuration file.

### FilesystemLogsPathAvailableINodes {#filesystemlogspathavailableinodes}

The number of available inodes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathTotalBytes {#filesystemlogspathtotalbytes}

The size of the volume where ClickHouse logs path is mounted, in bytes. It's recommended to have at least 10 GB for logs.

### FilesystemLogsPathTotalINodes {#filesystemlogspathtotalinodes}

The total number of inodes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathUsedBytes {#filesystemlogspathusedbytes}

Used bytes on the volume where ClickHouse logs path is mounted.

### FilesystemLogsPathUsedINodes {#filesystemlogspathusedinodes}

The number of used inodes on the volume where ClickHouse logs path is mounted.

### FilesystemMainPathAvailableBytes {#filesystemmainpathavailablebytes}

Available bytes on the volume where the main ClickHouse path is mounted.

### FilesystemMainPathAvailableINodes {#filesystemmainpathavailableinodes}

The number of available inodes on the volume where the main ClickHouse path is mounted. If it is close to zero, it indicates a misconfiguration, and you will get 'no space left on device' even when the disk is not full.

### FilesystemMainPathTotalBytes {#filesystemmainpathtotalbytes}

The size of the volume where the main ClickHouse path is mounted, in bytes.

### FilesystemMainPathTotalINodes {#filesystemmainpathtotalinodes}

The total number of inodes on the volume where the main ClickHouse path is mounted. If it is less than 25 million, it indicates a misconfiguration.

### FilesystemMainPathUsedBytes {#filesystemmainpathusedbytes}

Used bytes on the volume where the main ClickHouse path is mounted.

### FilesystemMainPathUsedINodes {#filesystemmainpathusedinodes}

The number of used inodes on the volume where the main ClickHouse path is mounted. This value mostly corresponds to the number of files.

### HTTPThreads {#httpthreads}

Number of threads in the server of the HTTP interface (without TLS).

### InterserverThreads {#interserverthreads}

Number of threads in the server of the replicas communication protocol (without TLS).

### Jitter {#jitter}

The difference in time the thread for calculation of the asynchronous metrics was scheduled to wake up and the time it was in fact, woken up. A proxy-indicator of overall system latency and responsiveness.

### LoadAverage*N* {#loadaveragen}

The whole system load, averaged with exponential smoothing over 1 minute. The load represents the number of threads across all the processes (the scheduling entities of the OS kernel), that are currently running by CPU or waiting for IO, or ready to run but not being scheduled at this point of time. This number includes all the processes, not only clickhouse-server. The number can be greater than the number of CPU cores, if the system is overloaded, and many processes are ready to run but waiting for CPU or IO.

### MMapCacheCells {#mmapcachecells}

The number of files opened with `mmap` (mapped in memory). This is used for queries with the setting `local_filesystem_read_method` set to  `mmap`. The files opened with `mmap` are kept in the cache to avoid costly TLB flushes.

### MarkCacheBytes {#markcachebytes}

Total size of mark cache in bytes

### MarkCacheFiles {#markcachefiles}

Total number of mark files cached in the mark cache

### MaxPartCountForPartition {#maxpartcountforpartition}

Maximum number of parts per partition across all partitions of all tables of MergeTree family. Values larger than 300 indicates misconfiguration, overload, or massive data loading.

### MemoryCode {#memorycode}

The amount of virtual memory mapped for the pages of machine code of the server process, in bytes.

### MemoryDataAndStack {#memorydataandstack}

The amount of virtual memory mapped for the use of stack and for the allocated memory, in bytes. It is unspecified whether it includes the per-thread stacks and most of the allocated memory, that is allocated with the 'mmap' system call. This metric exists only for completeness reasons. I recommend to use the `MemoryResident` metric for monitoring.

### MemoryResidentMax {#memoryresidentmax}

Maximum amount of physical memory used by the server process, in bytes.

### MemoryResident {#memoryresident}

The amount of physical memory used by the server process, in bytes.

### MemoryShared {#memoryshared}

The amount of memory used by the server process, that is also shared by another processes, in bytes. ClickHouse does not use shared memory, but some memory can be labeled by OS as shared for its own reasons. This metric does not make a lot of sense to watch, and it exists only for completeness reasons.

### MemoryVirtual {#memoryvirtual}

The size of the virtual address space allocated by the server process, in bytes. The size of the virtual address space is usually much greater than the physical memory consumption, and should not be used as an estimate for the memory consumption. The large values of this metric are totally normal, and makes only technical sense.

### MySQLThreads {#mysqlthreads}

Number of threads in the server of the MySQL compatibility protocol.

### NetworkReceiveBytes_*name* {#networkreceivebytes_name}

 Number of bytes received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceiveDrop_*name* {#networkreceivedrop_name}

 Number of bytes a packet was dropped while received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceiveErrors_*name* {#networkreceiveerrors_name}

 Number of times error happened receiving via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkReceivePackets_*name* {#networkreceivepackets_name}

 Number of network packets received via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendBytes_*name* {#networksendbytes_name}

 Number of bytes sent via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendDrop_*name* {#networksenddrop_name}

 Number of times a packed was dropped while sending via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendErrors_*name* {#networksenderrors_name}

 Number of times error (e.g. TCP retransmit) happened while sending via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NetworkSendPackets_*name* {#networksendpackets_name}

 Number of network packets sent via the network interface. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### NumberOfDatabases {#numberofdatabases}

Total number of databases on the server.

### NumberOfDetachedByUserParts {#numberofdetachedbyuserparts}

The total number of parts detached from MergeTree tables by users with the `ALTER TABLE DETACH` query (as opposed to unexpected, broken or ignored parts). The server does not care about detached parts, and they can be removed.

### NumberOfDetachedParts {#numberofdetachedparts}

The total number of parts detached from MergeTree tables. A part can be detached by a user with the `ALTER TABLE DETACH` query or by the server itself it the part is broken, unexpected or unneeded. The server does not care about detached parts, and they can be removed.

### NumberOfTables {#numberoftables}

Total number of tables summed across the databases on the server, excluding the databases that cannot contain MergeTree tables. The excluded database engines are those who generate the set of tables on the fly, like `Lazy`, `MySQL`, `PostgreSQL`, `SQlite`.

### OSContextSwitches {#oscontextswitches}

The number of context switches that the system underwent on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSGuestNiceTime {#osguestnicetime}

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel, when a guest was set to a higher priority (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestNiceTimeCPU_*N* {#osguestnicetimecpu_n}

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel, when a guest was set to a higher priority (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestNiceTimeNormalized {#osguestnicetimenormalized}

The value is similar to `OSGuestNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSGuestTime {#osguesttime}

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestTimeCPU_*N* {#osguesttimecpu_n}

The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This metric is irrelevant for ClickHouse, but still exists for completeness. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSGuestTimeNormalized {#osguesttimenormalized}

The value is similar to `OSGuestTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSIOWaitTime {#osiowaittime}

The ratio of time the CPU core was not running the code but when the OS kernel did not run any other process on this CPU as the processes were waiting for IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIOWaitTimeCPU_*N* {#osiowaittimecpu_n}

The ratio of time the CPU core was not running the code but when the OS kernel did not run any other process on this CPU as the processes were waiting for IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIOWaitTimeNormalized {#osiowaittimenormalized}

The value is similar to `OSIOWaitTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSIdleTime {#osidletime}

The ratio of time the CPU core was idle (not even ready to run a process waiting for IO) from the OS kernel standpoint. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This does not include the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIdleTimeCPU_*N* {#osidletimecpu_n}

The ratio of time the CPU core was idle (not even ready to run a process waiting for IO) from the OS kernel standpoint. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This does not include the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIdleTimeNormalized {#osidletimenormalized}

The value is similar to `OSIdleTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSInterrupts {#osinterrupts}

The number of interrupts on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSIrqTime {#osirqtime}

The ratio of time spent for running hardware interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate hardware misconfiguration or a very high network load. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIrqTimeCPU_*N* {#osirqtimecpu_n}

The ratio of time spent for running hardware interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate hardware misconfiguration or a very high network load. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSIrqTimeNormalized {#osirqtimenormalized}

The value is similar to `OSIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSMemoryAvailable {#osmemoryavailable}

The amount of memory available to be used by programs, in bytes. This is very similar to the `OSMemoryFreePlusCached` metric. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryBuffers {#osmemorybuffers}

The amount of memory used by OS kernel buffers, in bytes. This should be typically small, and large values may indicate a misconfiguration of the OS. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryCached {#osmemorycached}

The amount of memory used by the OS page cache, in bytes. Typically, almost all available memory is used by the OS page cache - high values of this metric are normal and expected. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryFreePlusCached {#osmemoryfreepluscached}

The amount of free memory plus OS page cache memory on the host system, in bytes. This memory is available to be used by programs. The value should be very similar to `OSMemoryAvailable`. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryFreeWithoutCached {#osmemoryfreewithoutcached}

The amount of free memory on the host system, in bytes. This does not include the memory used by the OS page cache memory, in bytes. The page cache memory is also available for usage by programs, so the value of this metric can be confusing. See the `OSMemoryAvailable` metric instead. For convenience, we also provide the `OSMemoryFreePlusCached` metric, that should be somewhat similar to OSMemoryAvailable. See also https://www.linuxatemyram.com/. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSMemoryTotal {#osmemorytotal}

The total amount of memory on the host system, in bytes.

### OSNiceTime {#osnicetime}

The ratio of time the CPU core was running userspace code with higher priority. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSNiceTimeCPU_*N* {#osnicetimecpu_n}

The ratio of time the CPU core was running userspace code with higher priority. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSNiceTimeNormalized {#osnicetimenormalized}

The value is similar to `OSNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSOpenFiles {#osopenfiles}

The total number of opened files on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesBlocked {#osprocessesblocked}

Number of threads blocked waiting for I/O to complete (`man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesCreated {#osprocessescreated}

The number of processes created. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSProcessesRunning {#osprocessesrunning}

The number of runnable (running or ready to run) threads by the operating system. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server.

### OSSoftIrqTime {#ossoftirqtime}

The ratio of time spent for running software interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate inefficient software running on the system. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSoftIrqTimeCPU_*N* {#ossoftirqtimecpu_n}

The ratio of time spent for running software interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. A high number of this metric may indicate inefficient software running on the system. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSoftIrqTimeNormalized {#ossoftirqtimenormalized}

The value is similar to `OSSoftIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSStealTime {#osstealtime}

The ratio of time spent in other operating systems by the CPU when running in a virtualized environment. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Not every virtualized environments present this metric, and most of them don't. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSStealTimeCPU_*N* {#osstealtimecpu_n}

The ratio of time spent in other operating systems by the CPU when running in a virtualized environment. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. Not every virtualized environments present this metric, and most of them don't. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSStealTimeNormalized {#osstealtimenormalized}

The value is similar to `OSStealTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSSystemTime {#ossystemtime}

The ratio of time the CPU core was running OS kernel (system) code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSystemTimeCPU_*N* {#ossystemtimecpu_n}

The ratio of time the CPU core was running OS kernel (system) code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSSystemTimeNormalized {#ossystemtimenormalized}

The value is similar to `OSSystemTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### OSThreadsRunnable {#osthreadsrunnable}

The total number of 'runnable' threads, as the OS kernel scheduler seeing it.

### OSThreadsTotal {#osthreadstotal}

The total number of threads, as the OS kernel scheduler seeing it.

### OSUptime {#osuptime}

The uptime of the host server (the machine where ClickHouse is running), in seconds.

### OSUserTime {#osusertime}

The ratio of time the CPU core was running userspace code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This includes also the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSUserTimeCPU_*N* {#osusertimecpu_n}

The ratio of time the CPU core was running userspace code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server. This includes also the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core). The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores].

### OSUserTimeNormalized {#osusertimenormalized}

The value is similar to `OSUserTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores. This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric.

### PostgreSQLThreads {#postgresqlthreads}

Number of threads in the server of the PostgreSQL compatibility protocol.

### QueryCacheBytes {#querycachebytes}

Total size of the query cache in bytes.

### QueryCacheEntries {#querycacheentries}

Total number of entries in the query cache.

### ReplicasMaxAbsoluteDelay {#replicasmaxabsolutedelay}

Maximum difference in seconds between the most fresh replicated part and the most fresh data part still to be replicated, across Replicated tables. A very high value indicates a replica with no data.

### ReplicasMaxInsertsInQueue {#replicasmaxinsertsinqueue}

Maximum number of INSERT operations in the queue (still to be replicated) across Replicated tables.

### ReplicasMaxMergesInQueue {#replicasmaxmergesinqueue}

Maximum number of merge operations in the queue (still to be applied) across Replicated tables.

### ReplicasMaxQueueSize {#replicasmaxqueuesize}

Maximum queue size (in the number of operations like get, merge) across Replicated tables.

### ReplicasMaxRelativeDelay {#replicasmaxrelativedelay}

Maximum difference between the replica delay and the delay of the most up-to-date replica of the same table, across Replicated tables.

### ReplicasSumInsertsInQueue {#replicassuminsertsinqueue}

Sum of INSERT operations in the queue (still to be replicated) across Replicated tables.

### ReplicasSumMergesInQueue {#replicassummergesinqueue}

Sum of merge operations in the queue (still to be applied) across Replicated tables.

### ReplicasSumQueueSize {#replicassumqueuesize}

Sum queue size (in the number of operations like get, merge) across Replicated tables.

### TCPThreads {#tcpthreads}

Number of threads in the server of the TCP protocol (without TLS).

### Temperature_*N* {#temperature_n}

The temperature of the corresponding device in ℃. A sensor can return an unrealistic value. Source: `/sys/class/thermal`

### Temperature_*name* {#temperature_name}

The temperature reported by the corresponding hardware monitor and the corresponding sensor in ℃. A sensor can return an unrealistic value. Source: `/sys/class/hwmon`

### TotalBytesOfMergeTreeTables {#totalbytesofmergetreetables}

Total amount of bytes (compressed, including data and indices) stored in all tables of MergeTree family.

### TotalPartsOfMergeTreeTables {#totalpartsofmergetreetables}

Total amount of data parts in all tables of MergeTree family. Numbers larger than 10 000 will negatively affect the server startup time, and it may indicate unreasonable choice of the partition key.

### TotalPrimaryKeyBytesInMemory {#totalprimarykeybytesinmemory}

The total amount of memory (in bytes) used by primary key values (only takes active parts into account).

### TotalPrimaryKeyBytesInMemoryAllocated {#totalprimarykeybytesinmemoryallocated}

The total amount of memory (in bytes) reserved for primary key values (only takes active parts into account).

### TotalRowsOfMergeTreeTables {#totalrowsofmergetreetables}

Total amount of rows (records) stored in all tables of MergeTree family.

### UncompressedCacheBytes {#uncompressedcachebytes}

Total size of uncompressed cache in bytes. Uncompressed cache does not usually improve the performance and should be mostly avoided.

### UncompressedCacheCells {#uncompressedcachecells}

Total number of entries in the uncompressed cache. Each entry represents a decompressed block of data. Uncompressed cache does not usually improve performance and should be mostly avoided.

### Uptime {#uptime}

The server uptime in seconds. It includes the time spent for server initialization before accepting connections.

### jemalloc.active {#jemallocactive}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.allocated {#jemallocallocated}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.dirty_purged {#jemallocarenasalldirty_purged}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.muzzy_purged {#jemallocarenasallmuzzy_purged}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pactive {#jemallocarenasallpactive}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pdirty {#jemallocarenasallpdirty}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.arenas.all.pmuzzy {#jemallocarenasallpmuzzy}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.num_runs {#jemallocbackground_threadnum_runs}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.num_threads {#jemallocbackground_threadnum_threads}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.background_thread.run_intervals {#jemallocbackground_threadrun_intervals}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.epoch {#jemallocepoch}

An internal incremental update number of the statistics of jemalloc (Jason Evans' memory allocator), used in all other `jemalloc` metrics.

### jemalloc.mapped {#jemallocmapped}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.metadata {#jemallocmetadata}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.metadata_thp {#jemallocmetadata_thp}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.resident {#jemallocresident}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.retained {#jemallocretained}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

### jemalloc.prof.active {#jemallocprofactive}

An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html

**See Also**

- [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
- [system.metrics](/operations/system-tables/metrics) — Contains instantly calculated metrics.
- [system.events](/operations/system-tables/events) — Contains a number of events that have occurred.
- [system.metric_log](/operations/system-tables/metric_log) — Contains a history of metrics values from tables `system.metrics` and `system.events`.
