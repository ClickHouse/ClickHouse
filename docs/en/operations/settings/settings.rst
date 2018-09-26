max_block_size
--------------
In ClickHouse, data is processed by blocks (sets of column parts). The internal processing cycles for a single block are efficient enough, but there are noticeable expenditures on each block. 'max_block_size' is a recommendation for what size of block (in number of rows) to load from tables. The block size shouldn't be too small, so that the expenditures on each block are still noticeable, but not too large, so that the query with LIMIT that is completed after the first block is processed quickly, so that too much memory isn't consumed when extracting a large number of columns in multiple threads, and so that at least some cache locality is preserved.

By default, it is 65,536.

Blocks the size of 'max_block_size' are not always loaded from the table. If it is obvious that less data needs to be retrieved, a smaller block is processed.

max_insert_block_size
---------------------
The size of blocks to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ``max_insert_block_size`` setting on the server doesn't affect the size of the inserted blocks.
The setting also doesn't have a purpose when using INSERT SELECT, since data is inserted in the same blocks that are formed after SELECT.

By default, it is 1,048,576.

This is slightly more than 'max_block_size'. The reason for this is because certain table engines (\*MergeTree) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, \*MergeTree tables sort data during insertion, and a large enough block size allows sorting more data in RAM.

max_threads
-----------
The maximum number of query processing threads
- excluding threads for retrieving data from remote servers (see the ``max_distributed_connections`` parameter).

This parameter applies to threads that perform the same stages of the query execution pipeline in parallel.
For example, if reading from a table, evaluating expressions with functions, filtering with WHERE and pre-aggregating for GROUP BY can all be done in parallel using at least ``max_threads`` number of threads, then 'max_threads' are used.

By default, 8.

If less than one SELECT query is normally run on a server at a time, set this parameter to a value slightly less than the actual number of processor cores.

For queries that are completed quickly because of a LIMIT, you can set a lower ``max_threads``. For example, if the necessary number of entries are located in every block and ``max_threads = 8``, 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the ``max_threads`` value, the less memory is consumed.

max_compress_block_size
-----------------------
The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, ``1,048,576 (1 MiB)``. If the size is reduced, the compression rate is significantly reduced, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. There usually isn't any reason to change this setting.

Don't confuse blocks for compression (a chunk of memory consisting of bytes) and blocks for query processing (a set of rows from a table).

min_compress_block_size
-----------------------
For \*MergeTree tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least ``min_compress_block_size``. By default, 65,536.

The actual size of the block, if the uncompressed data less than ``max_compress_block_size`` is no less than this value and no less than the volume of data for one mark.

Let's look at an example. Assume that ``index_granularity`` was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since ``min_compress_block_size = 65,536``, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won't be decompressed.

There usually isn't any reason to change this setting.

max_query_size
--------------
The maximum part of a query that can be taken to RAM for parsing with the SQL parser.
The INSERT query also contains data for INSERT that is processed by a separate stream parser (that consumes O(1) RAM), which is not included in this restriction.

By default, 256 KiB.

interactive_delay
-----------------
The interval in microseconds for checking whether request execution has been canceled and sending the progress.

By default, 100,000 (check for canceling and send progress ten times per second).

connect_timeout
---------------

receive_timeout
---------------

send_timeout
------------
Timeouts in seconds on the socket used for communicating with the client.

By default, 10, 300, 300.

poll_interval
-------------
Lock in a wait loop for the specified number of seconds.

By default, 10.

max_distributed_connections
---------------------------
The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 100.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

distributed_connections_pool_size
---------------------------------
The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 128.

connect_timeout_with_failover_ms
--------------------------------
The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the 'shard' and 'replica' sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

By default, 50.

connections_with_failover_max_tries
-----------------------------------
The maximum number of connection attempts with each replica, for the Distributed table engine.

By default, 3.

extremes
--------
Whether to count extreme values (the minimums and maximums in columns of a query result).
Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section "Extreme values".

use_uncompressed_cache
----------------------
Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
The uncompressed cache (only for tables in the MergeTree family) allows significantly reducing latency and increasing throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the ``uncompressed_cache_size`` configuration parameter (only set in the config file) - the size of uncompressed cache blocks. 
By default, it is 8 GiB. The uncompressed cache is filled in as needed; the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically in order to save space for truly small queries. So you can keep the ``use_uncompressed_cache`` setting always set to 1.

replace_running_query
---------------------
When using the HTTP interface, the 'query_id' parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same 'query_id' already exists at this time, the behavior depends on the 'replace_running_query' parameter.

``0`` (default) - Throw an exception (don't allow the query to run if a query with the same 'query_id' is already running).
``1`` - Cancel the old query and start running the new one.

Yandex.Metrica uses this parameter set to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn't finished yet, it should be canceled.

load_balancing
--------------
Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.

random (by default)
~~~~~~~~~~~~~~~~~~~
The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to any one of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

nearest_hostname
~~~~~~~~~~~~~~~~
The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a host name that is most similar to the server's host name in the config file (for the number of different characters in identical positions, up to the minimum length of both host names).

As an example, example01-01-1 and example01-01-2.yandex.ru are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem a little stupid, but it doesn't use external data about network topology, and it doesn't compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

in_order
~~~~~~~~
Replicas are accessed in the same order as they are specified. The number of errors does not matter. This method is appropriate when you know exactly which replica is preferable.

totals_mode
-----------
How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present.
See the section "WITH TOTALS modifier".

totals_auto_threshold
---------------------
The threshold for ``totals_mode = 'auto'``.
See the section "WITH TOTALS modifier".

default_sample
--------------
A floating-point number from 0 to 1. By default, 1.
Allows setting a default sampling coefficient for all SELECT queries.
(For tables that don't support sampling, an exception will be thrown.)
If set to 1, default sampling is not performed.

max_parallel_replicas
---------------------
The maximum number of replicas of each shard used when the query is executed.
For consistency (to get different parts of the same partition), this option only works for the specified sampling key.
The lag of the replicas is not controlled.

compile
-------
Enable query compilation. The default is 0 (disabled).

Compilation is provided for only part of the request processing pipeline - for the first aggregation step (GROUP BY).
In the event that this part of the pipeline was compiled, the query can work faster, by deploying short loops and inlining the aggregate function calls. The maximum performance increase (up to four times in rare cases) is achieved on queries with several simple aggregate functions. Typically, the performance gain is negligible. In very rare cases, the request may be slowed down.

min_count_to_compile
--------------------
After how many times, when the compiled piece of code could come in handy, perform its compilation. The default is 3.
In case the value is zero, the compilation is executed synchronously, and the request will wait for the compilation process to finish before continuing. This can be used for testing, otherwise use values ​​starting with 1. Typically, compilation takes about 5-10 seconds.
If the value is 1 or more, the compilation is performed asynchronously, in a separate thread. If the result is ready, it will be immediately used, including those already running at the moment requests.

The compiled code is required for each different combination of aggregate functions used in the query and the type of keys in GROUP BY.
The compilation results are saved in the build directory as .so files. The number of compilation results is unlimited, since they do not take up much space. When the server is restarted, the old results will be used, except for the server update - then the old results are deleted.

input_format_skip_unknown_fields
--------------------------------
If the parameter is true, INSERT operation will skip columns with unknown names from input.
Otherwise, an exception will be generated, it is default behavior.
The parameter works only for JSONEachRow and TSKV input formats.

output_format_json_quote_64bit_integers
---------------------------------------
If the parameter is true (default value), UInt64 and Int64 numbers are printed as quoted strings in all JSON output formats.
Such behavior is compatible with most JavaScript interpreters that stores all numbers as double-precision floating point numbers.
Otherwise, they are printed as regular numbers.

stream_flush_interval_ms
------------------------
This setting only applies in cases when the server forms blocks from streaming table engines.
Either the timeout happens, or the stream produces ``max_insert_block_size`` rows.

By default, 7500.

Lower value results in stream flushing to table more often, so the data appears in the destination table faster.
Setting the value too low may result in excessive insertion frequency and lower ingestion efficiency.

schema
------
This parameter only applies when used in conjunction with formats requiring a schema definition, for example Cap'n Proto. The parameter value is specific to the format.

temporary_live_view_timeout
---------------------------
Timeout in seconds after which temporary live view table is deleted if there are no active WATCH queries.

temporary_live_channel_timeout
------------------------------
Timeout in seconds after which temporary live channel table is deleted if there are no active WATCH queries.

heartbeat_delay
---------------
The interval in microseconds of inactivity after which heartbeat is sent to keep live query or channel WATCH query
connection alive.

alter_channel_wait_ms
---------------------
The maximum timeout to wait in milliseconds for ALTER CHANNEL query to complete.