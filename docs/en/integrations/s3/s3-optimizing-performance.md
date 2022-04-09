---
sidebar_label: Optimizing S3 Performance 
sidebar_position: 5
description: Optimizing S3 Performance with ClickHouse
---

# Optimizing for Performance

## Measuring Performance

Before making any changes to improve performance, ensure you measure appropriately. As S3 API calls are sensitive to latency and may impact client timings, use the query log for performance metrics, i.e., system.query_log. 

If measuring the performance of SELECT queries, where large volumes of data are returned to the client, either utilize the [null format](https://clickhouse.com/docs/en/interfaces/formats/#null) for queries or direct results to the [Null engine](https://clickhouse.com/docs/en/engines/table-engines/special/null/). This should avoid the client being overwhelmed with data and network saturation.

## Region Locality

Ensure your buckets are located in the same region as your ClickHouse instances. This simple optimization can dramatically improve throughput performance, especially if you deploy your ClickHouse instances on AWS infrastructure.

## Using Threads

Read performance on s3 will scale linearly with the number of cores, provided you are not limited by network bandwidth or local IO. Increasing the number of threads also has memory overhead permutations that users should be aware of. The following can be modified to improve throughput performance potentially:

* Usually, the default value of `max_threads` is sufficient, i.e., the number of cores. If the amount of memory used for a query is high, and this needs to be reduced, or the LIMIT on results is low, this value can be set lower. Users with plenty of memory may wish to experiment with increasing this value for possible higher read throughput from s3. Typically this is only beneficial on machines with lower core counts, i.e., &lt; 10. The benefit from further parallelization typically diminishes as other resources act as a bottleneck, e.g., network.
* If performing an `INSERT INTO x SELECT` request, note that the number of threads will be set to 1 as dictated by the setting [max_insert_threads](https://clickhouse.com/docs/en/operations/settings/settings/#settings-max_threads). Provided max_threads is greater than 1 (confirm with `SELECT * FROM system.settings WHERE name='max_threads'`), increasing this will improve insert performance at the expense of memory. Increase with caution due to memory consumption overheads. This value should not be as high as the max_threads as resources are consumed on background merges. Furthermore, not all target engines (MergeTree does) support parallel inserts. Finally, parallel inserts invariably cause more parts, slowing down subsequent reads. Increase with caution.
* For low memory scenarios, consider lowering `max_insert_delayed_streams_for_parallel_write` if inserting into s3.
* Versions of ClickHouse before 22.3.1 only parallelized reads across multiple files when using the s3 function or s3 table engine. This required the user to ensure files were split into chunks on s3 and read using a glob pattern to achieve optimal read performance. Later versions now parallelize downloads within a file. 
* Assuming sufficient memory (test!), increasing [min_insert_block_size_rows](https://clickhouse.com/docs/en/operations/settings/settings/#min-insert-block-size-rows) can improve insert throughput.
* In low thread count scenarios, users may benefit from setting `remote_filesystem_read_method` to "read" to cause the synchronous reading of files from s3.

## Formats

ClickHouse can read files stored in s3 buckets in the [supported formats](https://clickhouse.com/docs/en/interfaces/formats/#data-formatting) using the s3 function and s3 engine. If reading raw files, some of these formats have distinct advantages:


* Formats with encoded column names such as Native, Parquet, CSVWithNames, and TabSeparatedWithNames will be less verbose to query since the user will not be required to specify the column name is the s3 function. The column names allow this information to be inferred.
* Formats will differ in performance with respect to read and write throughputs. Native and parquet represent the most optimal formats for read performance since they are already column orientated and more compact. The native format additionally benefits from alignment with how ClickHouse stores data in memory - thus reducing processing overhead as data is streamed into ClickHouse.
* The block size will often impact the latency of reads on large files. This is very apparent if you only sample the data, e.g., returning the top N rows. In the case of formats such as CSV and TSV, files must be parsed to return a set of rows. Formats such as Native and Parquet will allow faster sampling as a result.
* Each compression format brings pros and cons, often balancing the compression level for speed and biasing compression or decompression performance. If compressing raw files such as CSV or TSV, lz4 offers the fastest decompression performance, sacrificing the compression level. Gzip typically compresses better at the expense of slightly slower read speeds. Xz takes this further by usually offering the best compression with the slowest compression and decompression performance. If exporting, Gz and lz4 offer comparable compression speeds. Balance this against your connection speeds. Any gains from faster decompression or compression will be easily negated by a slower connection to your s3 buckets.
* Formats such as native or parquet do not typically justify the overhead of compression. Any savings in data size are likely to be minimal since these formats are inherently compact. The time spent compressing and decompressing will rarely offset network transfer times - especially since s3 is globally available with higher network bandwidth.


Internally the ClickHouse merge tree uses two primary storage formats: [Wide and Compact](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-data-storage). While the current implementation uses the default behavior of ClickHouse - controlled through the settings `min_bytes_for_wide_part` and `min_rows_for_wide_part`; we expect behavior to diverge for s3 in the future releases, e.g., a larger default value of min_bytes_for_wide_part encouraging a more Compact format and thus fewer files. Users may now wish to tune these settings when using exclusively s3 storage. 

## Scaling with Nodes

Users will often have more than one node of ClickHouse available. While users can scale vertically, improving s3 throughput linearly with the number of cores, horizontal scaling is often necessary due to hardware availability and cost-efficiency.

The replication of an s3 backed Merge Tree is supported through zero copy replication. 

Utilizing a cluster for s3 reads requires using the s3Cluster function as described in [Utilizing Clusters](./s3-table-functions#utilizing-clusters). While this allows reads to be distributed across nodes, thread settings will not currently be sent to all nodes as of 22.3.1. For example, if the following query was executed against a node, only the receiving initiator node will respect the max_insert_threads setting.

```sql
INSERT INTO default.trips_all SELECT * FROM s3Cluster('events', 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames') SETTINGS max_insert_threads=8;
```

To ensure this setting is used, the following would need to be added to each nodes config.xml file (or under conf.d):

```xml
<clickhouse>  
  <profiles>     
    <default>
      <max_insert_threads>8</max_insert_threads>
    </default>
  </profiles>
</clickhouse>
```

