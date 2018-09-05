# Performance

According to internal testing results by Yandex, ClickHouse shows the best performance for comparable operating scenarios among systems of its class that were available for testing. This includes the highest throughput for long queries, and the lowest latency on short queries. Testing results are shown on a [separate page](https://clickhouse.yandex/benchmark.html).

There are a lot of independent benchmarks that confirm this as well. You can look it up on your own or here is the small [collection of independent benchmark links](https://clickhouse.yandex/#independent-benchmarks).

## Throughput for a single large query

Throughput can be measured in rows per second or in megabytes per second. If the data is placed in the page cache, a query that is not too complex is processed on modern hardware at a speed of approximately 2-10 GB/s of uncompressed data on a single server (for the simplest cases, the speed may reach 30 GB/s). If data is not placed in the page cache, the speed is bound by the disk subsystem and how well the data has been compressed. For example, if the disk subsystem allows reading data at 400 MB/s, and the data compression rate is 3, the speed will be around 1.2 GB/s. To get the speed in rows per second, divide the speed in bytes per second by the total size of the columns used in the query. For example, if 10 bytes of columns are extracted, the speed will be around 100-200 million rows per second.

The processing speed increases almost linearly for distributed processing, but only if the number of rows resulting from aggregation or sorting is not too large.

## Latency when processing short queries

If a query uses a primary key and does not select too many rows to process (hundreds of thousands), and does not use too many columns, we can expect less than 50 milliseconds of latency (single digits of milliseconds in the best case) if data is placed in the page cache. Otherwise, latency is calculated from the number of seeks. If you use rotating drives, for a system that is not overloaded, the approximate latency can be calculated by this formula: seek time (10 ms) \* number of columns queried \* number of data parts.

## Throughput when processing a large quantity of short queries

Under the same circumstances, ClickHouse can handle several hundred queries per second on a single server (up to several thousands in the best case). Since this scenario is not typical for analytical DBMSs, it is better to  expect a maximum of hundreds of queries per second.

## Performance when inserting data

It is recommended to insert data in batches of at least 1000 rows, or no more than a single request per second. When inserting to a MergeTree table from a tab-separated dump, the insertion speed will be from 50 to 200 MB/s. If the inserted rows are around 1 Kb in size, the speed will be from 50,000 to 200,000 rows per second. If the rows are small, the performance will be higher in rows per second (on Banner System data -`>` 500,000 rows per second; on Graphite data -`>` 1,000,000 rows per second). To improve performance, you can make multiple INSERT queries in parallel, and performance will increase linearly.

