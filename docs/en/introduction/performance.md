---
toc_priority: 6
toc_title: Performance
---

# Performance {#performance}

According to internal testing results at Yandex, ClickHouse shows the best performance (both the highest throughput for long queries and the lowest latency on short queries) for comparable operating scenarios among systems of its class that were available for testing. You can view the test results on a [separate page](https://clickhouse.tech/benchmark/dbms/).

Numerous independent benchmarks came to similar conclusions. They are not difficult to find using an internet search, or you can see [our small collection of related links](https://clickhouse.tech/#independent-benchmarks).

## Throughput for a Single Large Query {#throughput-for-a-single-large-query}

Throughput can be measured in rows per second or megabytes per second. If the data is placed in the page cache, a query that is not too complex is processed on modern hardware at a speed of approximately 2-10 GB/s of uncompressed data on a single server (for the most straightforward cases, the speed may reach 30 GB/s). If data is not placed in the page cache, the speed depends on the disk subsystem and the data compression rate. For example, if the disk subsystem allows reading data at 400 MB/s, and the data compression rate is 3, the speed is expected to be around 1.2 GB/s. To get the speed in rows per second, divide the speed in bytes per second by the total size of the columns used in the query. For example, if 10 bytes of columns are extracted, the speed is expected to be around 100-200 million rows per second.

The processing speed increases almost linearly for distributed processing, but only if the number of rows resulting from aggregation or sorting is not too large.

## Latency When Processing Short Queries {#latency-when-processing-short-queries}

If a query uses a primary key and does not select too many columns and rows to process (hundreds of thousands), you can expect less than 50 milliseconds of latency (single digits of milliseconds in the best case) if data is placed in the page cache. Otherwise, latency is mostly dominated by the number of seeks. If you use rotating disk drives, for a system that is not overloaded, the latency can be estimated with this formula: `seek time (10 ms) * count of columns queried * count of data parts`.

## Throughput When Processing a Large Quantity of Short Queries {#throughput-when-processing-a-large-quantity-of-short-queries}

Under the same conditions, ClickHouse can handle several hundred queries per second on a single server (up to several thousand in the best case). Since this scenario is not typical for analytical DBMSs, we recommend expecting a maximum of 100 queries per second.

## Performance When Inserting Data {#performance-when-inserting-data}

We recommend inserting data in packets of at least 1000 rows, or no more than a single request per second. When inserting to a MergeTree table from a tab-separated dump, the insertion speed can be from 50 to 200 MB/s. If the inserted rows are around 1 Kb in size, the speed will be from 50,000 to 200,000 rows per second. If the rows are small, the performance can be higher in rows per second (on Banner System data -`>` 500,000 rows per second; on Graphite data -`>` 1,000,000 rows per second). To improve performance, you can make multiple INSERT queries in parallel, which scales linearly.

{## [Original article](https://clickhouse.tech/docs/en/introduction/performance/) ##}
