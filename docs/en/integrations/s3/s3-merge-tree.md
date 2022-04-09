---
sidebar_label: S3 Backed MergeTree 
sidebar_position: 4
description: S3 Backed MergeTree
---

# S3 Backed MergeTree

:::note
This feature is currently experimental and undergoing improvements and experimentation to understand performance.
:::

The `s3` functions and associated table engine allow us to query data in S3 using familiar ClickHouse syntax. However, concerning data management features and performance, they are limited. There is no support for primary indexes, no-cache support, and files inserts need to be managed by the user.

ClickHouse recognizes that S3 represents an attraction storage operation: especially where query performance on “colder” data is less critical, and users seek to separate storage and compute. To help achieve this, support is provided for using S3 as the storage for a MergeTree engine. This will enable users to exploit the scalability and cost of benefits of S3 and the insert and query performance of the MergeTree engine.

## Storage Tiers

ClickHouse storage volumes allow physical disks to be abstracted from the MergeTree table engine. Any single volume can be composed of an ordered set of disks. Whilst principally allowing multiple block devices to be potentially used for data storage, this abstraction also allows other storage types, including S3. ClickHouse data parts can be moved between volumes and fill rates according to storage policies, thus creating the concept of storage tiers. 

Storage tiers unlock hot-cold architectures where the most recent data, which is typically also the most queried, requires only a small amount of space on high-performing storage, e.g., NVMe SSDs. As the data ages, SLAs for query times increase, as does query frequency. This fat tail of data can be stored on slower, less performant storage such as HDD or object storage such as S3.

## Creating a Disk

To utilize an S3 bucket as a disk, we must first declare it within the ClickHouse configuration file. Either extend config.xml or preferably provide a new file under conf.d. An example of an S3 disk declaration is shown below:

```xml
<clickhouse>
    <storage_configuration>
        ...
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://sample-bucket.s3.us-east-2.amazonaws.com/tables/</endpoint>
                <access_key_id>your_access_key_id</access_key_id>
                <secret_access_key>your_secret_access_key</secret_access_key>
                <region></region>
                <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
                <cache_enabled>true</cache_enabled>
            <data_cache_enabled>true</data_cache_enabled>	
                <cache_path>/var/lib/clickhouse/disks/s3/cache/</cache_path>
            </s3>
        </disks>
        ...
    </storage_configuration>
</clickhouse>

```

A complete list of settings relevant to this disk declaration can be found [here](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-s3). Note that credentials can be managed here using the same approaches described in [Managing credentials](./s3-table-engine#managing-credentials), i.e., the use_environment_credentials can be set to true in the above settings block to use IAM roles. Further information on the cache settings can be found under [Internals](#internals).

## Creating a Storage Policy

Once configured, this “disk” can be used by a storage volume declared within a policy. For the example below, we assume s3 is our only storage. This ignores more complex hot-cold architectures where data can be relocated based on TTLs and fill rates. 

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
            ...
            </s3>
        </disks>
        <policies>
            <s3_main>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_main>
        </policies>
    </storage_configuration>
</clickhouse>
```

## Creating a table

Assuming you have configured your disk to use a bucket with write access, you should be able to create a table such as in the example below. For purposes of brevity, we use a subset of the NYC taxi columns and stream data directly to the s3 backed table:

```sql
CREATE TABLE trips_s3
(
   `trip_id` UInt32,
   `pickup_date` Date,
   `pickup_datetime` DateTime,
   `dropoff_datetime` DateTime,
   `pickup_longitude` Float64,
   `pickup_latitude` Float64,
   `dropoff_longitude` Float64,
   `dropoff_latitude` Float64,
   `passenger_count` UInt8,
   `trip_distance` Float64,
   `tip_amount` Float32,
   `total_amount` Float32,
   `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime
SETTINGS index_granularity = 8192, storage_policy='s3_main'
```

```sql
INSERT INTO trips_s3 SELECT trip_id, pickup_date, pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude, passenger_count, trip_distance, tip_amount, total_amount, payment_type FROM s3('https://ch-nyc-taxi.s3.eu-west-3.amazonaws.com/tsv/trips_{0..9}.tsv.gz', 'TabSeparatedWithNames') LIMIT 1000000;
```

Depending on the hardware, this latter insert of 1m rows may take a few minutes to execute. You can confirm the progress via the system.processes table. Feel free to adjust the row count up to the limit of 10m and explore some sample queries.

```sql
SELECT passenger_count, avg(tip_amount) as avg_tip, avg(total_amount) as avg_amount FROM trips_s3 GROUP BY passenger_count;
```

## Modifying a Table

Occasionally users may need to modify the storage policy of a specific table. Whilst this is possible, it comes with limitations. The new target policy must contain all of the disks and volumes of the previous policy, i.e., data will not be migrated to satisfy a policy change. When validating these constraints, volumes and disks will be identified by their name, with attempts to violate resulting in an error. However, assuming you use the previous examples, the following changes are valid.

```xml
<policies>
   <s3_main>
       <volumes>
           <main>
               <disk>s3</disk>
           </main>
       </volumes>
   </s3_main>
   <s3_tiered>
       <volumes>
           <hot>
               <disk>default</disk>
           </hot>
           <main>
               <disk>s3</disk>
           </main>
       </volumes>
       <move_factor>0.2</move_factor>
   </s3_tiered>
</policies>
```

```sql
ALTER TABLE trips_s3 MODIFY SETTING storage_policy='s3_tiered'
```

Here we reuse the main volume in our new s3_tiered policy and introduce a new hot volume. This uses the default disk, which consists of only one disk configured via the parameter `<path>`. Note that our volume names and disks do not change.  New inserts to our table will reside on the default disk until this reaches move_factor * disk_size - at which data will be relocated to S3. 

## Handling Replication

For traditional disk-backed tables, we rely on ClickHouse to handle data replication via the ReplicatedTableEngine. Whilst for S3, this replication is inherently handled at the storage layer, local files are still held for the table on disk. Specifically, ClickHouse stores metadata data files on disk (see [Internals](#internals)) for further details. These files will be replicated if using a ReplicatedMergeTree in a process known as [Zero Copy Replication](https://clickhouse.com/docs/en/operations/storing-data/#zero-copy). This is enabled by default through the setting allow_remote_fs_zero_copy_replication. This is best illustrated below where the table exists on 2 ClickHouse nodes:


<img src={require('./images/s3_01.png').default} class="image" alt="Replicating S3 backed MergeTree" style={{width: '80%'}}/>

## Internals

## Read & Writes

The following notes cover the implementation of S3 interactions with ClickHouse. Whilst generally only informative, it may help the readers when [Optimizing for Performance](./s3-optimizing-performance):

* By default, the maximum number of query processing threads used by any stage of the query processing pipeline is equal to the number of cores. Some stages are more parallelizable than others, so this value provides an upper bound.  Multiple query stages may execute at once since data is streamed from the disk. The exact number of threads used for a query may thus exceed this. Modify through the setting [max_threads](https://clickhouse.com/docs/en/operations/settings/settings/#settings-max_threads).
* Reads on S3 are asynchronous by default. This behavior is determined by setting `remote_filesystem_read_method`, set to the value `threadpool` by default. When serving a request, ClickHouse reads granules in stripes. Each of these stripes potentially contain many columns. A thread will read the columns for their granules one by one. Rather than doing this synchronously, a prefetch is made for all columns before waiting for the data. This offers significant performance improvements over synchronous waits on each column. Users will not need to change this setting in most cases - see [Optimizing for Performance](./s3-optimizing-performance).
* For the s3 function and table, parallel downloading is determined by the values `max_download_threads` and `max_download_buffer_size`. Files will only be downloaded in parallel if their size is greater than the total buffer size combined across all threads. This is only available on versions > 22.3.1.
* Writes are performed in parallel, with a maximum of 100 concurrent file writing threads. `max_insert_delayed_streams_for_parallel_write`, which has a default value of 1000,  controls the number of S3 blobs written in parallel. Since a buffer is required for each file being written (~1MB), this effectively limits the memory consumption of an INSERT. It may be appropriate to lower this value in low server memory scenarios.


For further information on tuning threads, see [Optimizing for Performance](./s3-optimizing-performance).

Important: as of 22.3.1, there are two settings to enable the cache `data_cache_enabled` and `enable_filesystem_cache`. We recommend setting both of these 1 to enable the new cache behavior described, which supports the eviction of index files. To disable the eviction of index and mark files from the cache, we also recommend setting `cache_enabled=1`. 

To accelerate reads, s3 files are cached on the local filesystem by breaking files into segments. Any contiguous read segments are saved in the cache, with overlapping segments reused. Later versions will optionally allow writes resulting from INSERTs or merges to be stored in the cache via the option `enable_filesystem_cache_on_write_operations`. Where possible, the cache is reused for file reads. ClickHouse’s linear reads lend themselves to this caching strategy. Should a contiguous read result in a cache miss, the segment is downloaded and cached. Eviction occurs on an LRU basis per segment. The removal of a file also causes its removal from the cache. The setting `read_from_cache_if_exists_otherwise_bypass_cache` can be set to 1 for specific queries which you know are not cache efficient. These queries might be known to be unfriendly to the cache and result in heavy evictions.

The metadata for the cache (entries and last used time) is held in memory for fast access. On restarts of ClickHouse, this metadata is reconstructed from the files on disk with the loss of the last used time. In this case, the value is set to 0, causing random eviction until the values are fully populated. 

The max cache size can be specified in bytes through the setting `max_cache_size`. This defaults to 1GB (subject to change). Index and mark files can be evicted from the cache. The FS page cache can efficiently cache all files.

Enabling the cache can speed up first-time queries for which the data is not resident in the cache. If a query needs to re-access data that has been cached as part of its execution, the fs page cache can be utilized - thus avoiding re-reads from s3.

Finally, merges on data residing in s3 are potentially a performant bottleneck if not performed intelligently. Cached versions of files minimize merges performed directly on the remote storage. 
