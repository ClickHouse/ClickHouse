---
slug: /en/development/architecture
sidebar_label: Architecture Overview
sidebar_position: 62
---

# Overview of ClickHouse Architecture

ClickHouse is a true column-oriented DBMS. Data is stored by columns, and during the execution of arrays (vectors or chunks of columns).
Whenever possible, operations are dispatched on arrays, rather than on individual values. It is called “vectorized query execution” and it helps lower the cost of actual data processing.

> This idea is nothing new. It dates back to the `APL` (A programming language, 1957) and its descendants: `A +` (APL dialect), `J` (1990), `K` (1993), and `Q` (programming language from Kx Systems, 2003). Array programming is used in scientific data processing. Neither is this idea something new in relational databases: for example, it is used in the `VectorWise` system (also known as Actian Vector Analytic Database by Actian Corporation).

There are two different approaches for speeding up query processing: vectorized query execution and runtime code generation. The latter removes all indirection and dynamic dispatch. Neither of these approaches is strictly better than the other. Runtime code generation can be better when it fuses many operations, thus fully utilizing CPU execution units and the pipeline. Vectorized query execution can be less practical because it involves temporary vectors that must be written to the cache and read back. If the temporary data does not fit in the L2 cache, this becomes an issue. But vectorized query execution more easily utilizes the SIMD capabilities of the CPU. A [research paper](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) written by our friends shows that it is better to combine both approaches. ClickHouse uses vectorized query execution and has limited initial support for runtime code generation.

## Columns {#columns}

`IColumn` interface is used to represent columns in memory (actually, chunks of columns). This interface provides helper methods for the implementation of various relational operators. Almost all operations are immutable: they do not modify the original column, but create a new modified one. For example, the `IColumn :: filter` method accepts a filter byte mask. It is used for the `WHERE` and `HAVING` relational operators. Additional examples: the `IColumn :: permute` method to support `ORDER BY`, the `IColumn :: cut` method to support `LIMIT`.

Various `IColumn` implementations (`ColumnUInt8`, `ColumnString`, and so on) are responsible for the memory layout of columns. The memory layout is usually a contiguous array. For the integer type of columns, it is just one contiguous array, like `std :: vector`. For `String` and `Array` columns, it is two vectors: one for all array elements, placed contiguously, and a second one for offsets to the beginning of each array. There is also `ColumnConst` that stores just one value in memory, but looks like a column.

## Field {#field}

Nevertheless, it is possible to work with individual values as well. To represent an individual value, the `Field` is used. `Field` is just a discriminated union of `UInt64`, `Int64`, `Float64`, `String` and `Array`. `IColumn` has the `operator []` method to get the n-th value as a `Field`, and the `insert` method to append a `Field` to the end of a column. These methods are not very efficient, because they require dealing with temporary `Field` objects representing an individual value. There are more efficient methods, such as `insertFrom`, `insertRangeFrom`, and so on.

`Field` does not have enough information about a specific data type for a table. For example, `UInt8`, `UInt16`, `UInt32`, and `UInt64` are all represented as `UInt64` in a `Field`.

## Leaky Abstractions {#leaky-abstractions}

`IColumn` has methods for common relational transformations of data, but they do not meet all needs. For example, `ColumnUInt64` does not have a method to calculate the sum of two columns, and `ColumnString` does not have a method to run a substring search. These countless routines are implemented outside of `IColumn`.

Various functions on columns can be implemented in a generic, non-efficient way using `IColumn` methods to extract `Field` values, or in a specialized way using knowledge of inner memory layout of data in a specific `IColumn` implementation. It is implemented by casting functions to a specific `IColumn` type and deal with internal representation directly. For example, `ColumnUInt64` has the `getData` method that returns a reference to an internal array, then a separate routine reads or fills that array directly. We have “leaky abstractions” to allow efficient specializations of various routines.

## Data Types {#data_types}

`IDataType` is responsible for serialization and deserialization: for reading and writing chunks of columns or individual values in binary or text form. `IDataType` directly corresponds to data types in tables. For example, there are `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` and so on.

`IDataType` and `IColumn` are only loosely related to each other. Different data types can be represented in memory by the same `IColumn` implementations. For example, `DataTypeUInt32` and `DataTypeDateTime` are both represented by `ColumnUInt32` or `ColumnConstUInt32`. In addition, the same data type can be represented by different `IColumn` implementations. For example, `DataTypeUInt8` can be represented by `ColumnUInt8` or `ColumnConstUInt8`.

`IDataType` only stores metadata. For instance, `DataTypeUInt8` does not store anything at all (except virtual pointer `vptr`) and `DataTypeFixedString` stores just `N` (the size of fixed-size strings).

`IDataType` has helper methods for various data formats. Examples are methods to serialize a value with possible quoting, to serialize a value for JSON, and to serialize a value as part of the XML format. There is no direct correspondence to data formats. For example, the different data formats `Pretty` and `TabSeparated` can use the same `serializeTextEscaped` helper method from the `IDataType` interface.

## Block {#block}

A `Block` is a container that represents a subset (chunk) of a table in memory. It is just a set of triples: `(IColumn, IDataType, column name)`. During query execution, data is processed by `Block`s. If we have a `Block`, we have data (in the `IColumn` object), we have information about its type (in `IDataType`) that tells us how to deal with that column, and we have the column name. It could be either the original column name from the table or some artificial name assigned for getting temporary results of calculations.

When we calculate some function over columns in a block, we add another column with its result to the block, and we do not touch columns for arguments of the function because operations are immutable. Later, unneeded columns can be removed from the block, but not modified. It is convenient for the elimination of common subexpressions.

Blocks are created for every processed chunk of data. Note that for the same type of calculation, the column names and types remain the same for different blocks, and only column data changes. It is better to split block data from the block header because small block sizes have a high overhead of temporary strings for copying shared_ptrs and column names.

## Processors

See the description at [https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/IProcessor.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/IProcessor.h).

## Formats {#formats}

Data formats are implemented with processors.

## I/O {#io}

For byte-oriented input/output, there are `ReadBuffer` and `WriteBuffer` abstract classes. They are used instead of C++ `iostream`s. Don’t worry: every mature C++ project is using something other than `iostream`s for good reasons.

`ReadBuffer` and `WriteBuffer` are just a contiguous buffer and a cursor pointing to the position in that buffer. Implementations may own or not own the memory for the buffer. There is a virtual method to fill the buffer with the following data (for `ReadBuffer`) or to flush the buffer somewhere (for `WriteBuffer`). The virtual methods are rarely called.

Implementations of `ReadBuffer`/`WriteBuffer` are used for working with files and file descriptors and network sockets, for implementing compression (`CompressedWriteBuffer` is initialized with another WriteBuffer and performs compression before writing data to it), and for other purposes – the names `ConcatReadBuffer`, `LimitReadBuffer`, and `HashingWriteBuffer` speak for themselves.

Read/WriteBuffers only deal with bytes. There are functions from `ReadHelpers` and `WriteHelpers` header files to help with formatting input/output. For example, there are helpers to write a number in decimal format.

Let’s look at what happens when you want to write a result set in `JSON` format to stdout. You have a result set ready to be fetched from `IBlockInputStream`. You create `WriteBufferFromFileDescriptor(STDOUT_FILENO)` to write bytes to stdout. You create `JSONRowOutputStream`, initialized with that `WriteBuffer`, to write rows in `JSON` to stdout. You create `BlockOutputStreamFromRowOutputStream` on top of it, to represent it as `IBlockOutputStream`. Then you call `copyData` to transfer data from `IBlockInputStream` to `IBlockOutputStream`, and everything works. Internally, `JSONRowOutputStream` will write various JSON delimiters and call the `IDataType::serializeTextJSON` method with a reference to `IColumn` and the row number as arguments. Consequently, `IDataType::serializeTextJSON` will call a method from `WriteHelpers.h`: for example, `writeText` for numeric types and `writeJSONString` for `DataTypeString`.

## Tables {#tables}

The `IStorage` interface represents tables. Different implementations of that interface are different table engines. Examples are `StorageMergeTree`, `StorageMemory`, and so on. Instances of these classes are just tables.

The key `IStorage` methods are `read` and `write`. There are also `alter`, `rename`, `drop`, and so on. The `read` method accepts the following arguments: the set of columns to read from a table, the `AST` query to consider, and the desired number of streams to return. It returns one or multiple `IBlockInputStream` objects and information about the stage of data processing that was completed inside a table engine during query execution.

In most cases, the read method is only responsible for reading the specified columns from a table, not for any further data processing. All further data processing is done by the query interpreter and is outside the responsibility of `IStorage`.

But there are notable exceptions:

- The AST query is passed to the `read` method, and the table engine can use it to derive index usage and to read fewer data from a table.
- Sometimes the table engine can process data itself to a specific stage. For example, `StorageDistributed` can send a query to remote servers, ask them to process data to a stage where data from different remote servers can be merged, and return that preprocessed data. The query interpreter then finishes processing the data.

The table’s `read` method can return multiple `IBlockInputStream` objects to allow parallel data processing. These multiple block input streams can read from a table in parallel. Then you can wrap these streams with various transformations (such as expression evaluation or filtering) that can be calculated independently and create a `UnionBlockInputStream` on top of them, to read from multiple streams in parallel.

There are also `TableFunction`s. These are functions that return a temporary `IStorage` object to use in the `FROM` clause of a query.

To get a quick idea of how to implement your table engine, look at something simple, like `StorageMemory` or `StorageTinyLog`.

> As the result of the `read` method, `IStorage` returns `QueryProcessingStage` – information about what parts of the query were already calculated inside storage.

## Parsers {#parsers}

A hand-written recursive descent parser parses a query. For example, `ParserSelectQuery` just recursively calls the underlying parsers for various parts of the query. Parsers create an `AST`. The `AST` is represented by nodes, which are instances of `IAST`.

> Parser generators are not used for historical reasons.

## Interpreters {#interpreters}

Interpreters are responsible for creating the query execution pipeline from an `AST`. There are simple interpreters, such as `InterpreterExistsQuery` and `InterpreterDropQuery`, or the more sophisticated `InterpreterSelectQuery`. The query execution pipeline is a combination of block input or output streams. For example, the result of interpreting the `SELECT` query is the `IBlockInputStream` to read the result set from; the result of the `INSERT` query is the `IBlockOutputStream` to write data for insertion to, and the result of interpreting the `INSERT SELECT` query is the `IBlockInputStream` that returns an empty result set on the first read, but that copies data from `SELECT` to `INSERT` at the same time.

`InterpreterSelectQuery` uses `ExpressionAnalyzer` and `ExpressionActions` machinery for query analysis and transformations. This is where most rule-based query optimizations are done. `ExpressionAnalyzer` is quite messy and should be rewritten: various query transformations and optimizations should be extracted to separate classes to allow modular transformations of query.

## Functions {#functions}

There are ordinary functions and aggregate functions. For aggregate functions, see the next section.

Ordinary functions do not change the number of rows – they work as if they are processing each row independently. In fact, functions are not called for individual rows, but for `Block`’s of data to implement vectorized query execution.

There are some miscellaneous functions, like [blockSize](../sql-reference/functions/other-functions.md#blocksize-function-blocksize), [rowNumberInBlock](../sql-reference/functions/other-functions.md#rownumberinblock-function-rownumberinblock), and [runningAccumulate](../sql-reference/functions/other-functions.md#runningaccumulate-runningaccumulate), that exploit block processing and violate the independence of rows.

ClickHouse has strong typing, so there’s no implicit type conversion. If a function does not support a specific combination of types, it throws an exception. But functions can work (be overloaded) for many different combinations of types. For example, the `plus` function (to implement the `+` operator) works for any combination of numeric types: `UInt8` + `Float32`, `UInt16` + `Int8`, and so on. Also, some variadic functions can accept any number of arguments, such as the `concat` function.

Implementing a function may be slightly inconvenient because a function explicitly dispatches supported data types and supported `IColumns`. For example, the `plus` function has code generated by instantiation of a C++ template for each combination of numeric types, and constant or non-constant left and right arguments.

It is an excellent place to implement runtime code generation to avoid template code bloat. Also, it makes it possible to add fused functions like fused multiply-add or to make multiple comparisons in one loop iteration.

Due to vectorized query execution, functions are not short-circuited. For example, if you write `WHERE f(x) AND g(y)`, both sides are calculated, even for rows, when `f(x)` is zero (except when `f(x)` is a zero constant expression). But if the selectivity of the `f(x)` condition is high, and calculation of `f(x)` is much cheaper than `g(y)`, it’s better to implement multi-pass calculation. It would first calculate `f(x)`, then filter columns by the result, and then calculate `g(y)` only for smaller, filtered chunks of data.

## Aggregate Functions {#aggregate-functions}

Aggregate functions are stateful functions. They accumulate passed values into some state and allow you to get results from that state. They are managed with the `IAggregateFunction` interface. States can be rather simple (the state for `AggregateFunctionCount` is just a single `UInt64` value) or quite complex (the state of `AggregateFunctionUniqCombined` is a combination of a linear array, a hash table, and a `HyperLogLog` probabilistic data structure).

States are allocated in `Arena` (a memory pool) to deal with multiple states while executing a high-cardinality `GROUP BY` query. States can have a non-trivial constructor and destructor: for example, complicated aggregation states can allocate additional memory themselves. It requires some attention to creating and destroying states and properly passing their ownership and destruction order.

Aggregation states can be serialized and deserialized to pass over the network during distributed query execution or to write them on the disk where there is not enough RAM. They can even be stored in a table with the `DataTypeAggregateFunction` to allow incremental aggregation of data.

> The serialized data format for aggregate function states is not versioned right now. It is ok if aggregate states are only stored temporarily. But we have the `AggregatingMergeTree` table engine for incremental aggregation, and people are already using it in production. It is the reason why backward compatibility is required when changing the serialized format for any aggregate function in the future.

## Server {#server}

The server implements several different interfaces:

- An HTTP interface for any foreign clients.
- A TCP interface for the native ClickHouse client and for cross-server communication during distributed query execution.
- An interface for transferring data for replication.

Internally, it is just a primitive multithread server without coroutines or fibers. Since the server is not designed to process a high rate of simple queries but to process a relatively low rate of complex queries, each of them can process a vast amount of data for analytics.

The server initializes the `Context` class with the necessary environment for query execution: the list of available databases, users and access rights, settings, clusters, the process list, the query log, and so on. Interpreters use this environment.

We maintain full backward and forward compatibility for the server TCP protocol: old clients can talk to new servers, and new clients can talk to old servers. But we do not want to maintain it eternally, and we are removing support for old versions after about one year.

:::note
For most external applications, we recommend using the HTTP interface because it is simple and easy to use. The TCP protocol is more tightly linked to internal data structures: it uses an internal format for passing blocks of data, and it uses custom framing for compressed data. We haven’t released a C library for that protocol because it requires linking most of the ClickHouse codebase, which is not practical.
:::

## Configuration {#configuration}

ClickHouse Server is based on POCO C++ Libraries and uses `Poco::Util::AbstractConfiguration` to represent it's configuration. Configuration is held by `Poco::Util::ServerApplication` class inherited by `DaemonBase` class, which in turn is inherited by `DB::Server` class, implementing clickhouse-server itself. So config can be accessed by `ServerApplication::config()` method.

Config is read from multiple files (in XML or YAML format) and merged into single `AbstractConfiguration` by `ConfigProcessor` class. Configuration is loaded at server startup and can be reloaded later if one of config files is updated, removed or added. `ConfigReloader` class is responsible for periodic monitoring of these changes and reload procedure as well. `SYSTEM RELOAD CONFIG` query also triggers config to be reloaded.

For queries and subsystems other than `Server` config is accessible using `Context::getConfigRef()` method. Every subsystem that is capable of reloading it's config without server restart should register itself in reload callback in `Server::main()` method. Note that if newer config has an error, most subsystems will ignore new config, log warning messages and keep working with previously loaded config. Due to the nature of `AbstractConfiguration` it is not possible to pass reference to specific section, so `String config_prefix` is usually used instead.

## Threads and jobs {#threads-and-jobs}

To execute queries and do side activities ClickHouse allocates threads from one of thread pools to avoid frequent thread creation and destruction. There are a few thread pools, which are selected depending on a purpose and structure of a job:
  * Server pool for incoming client sessions.
  * Global thread pool for general purpose jobs, background activities and standalone threads.
  * IO thread pool for jobs that are mostly blocked on some IO and are not CPU-intensive.
  * Background pools for periodic tasks.
  * Pools for preemptable tasks that can be split into steps.

Server pool is a `Poco::ThreadPool` class instance defined in `Server::main()` method. It can have at most `max_connection` threads. Every thread is dedicated to a single active connection.

Global thread pool is `GlobalThreadPool` singleton class. To allocate thread from it `ThreadFromGlobalPool` is used. It has an interface similar to `std::thread`, but pulls thread from the global pool and does all necessary initialization. It is configured with the following settings:
  * `max_thread_pool_size` - limit on thread count in pool.
  * `max_thread_pool_free_size` - limit on idle thread count waiting for new jobs.
  * `thread_pool_queue_size` - limit on scheduled job count.

Global pool is universal and all pools described below are implemented on top of it. This can be thought of as a hierarchy of pools. Any specialized pool takes its threads from the global pool using `ThreadPool` class. So the main purpose of any specialized pool is to apply limit on the number of simultaneous jobs and do job scheduling. If there are more jobs scheduled than threads in a pool, `ThreadPool` accumulates jobs in a queue with priorities. Each job has an integer priority. Default priority is zero. All jobs with higher priority values are started before any job with lower priority value. But there is no difference between already executing jobs, thus priority matters only when the pool in overloaded.

IO thread pool is implemented as a plain `ThreadPool` accessible via `IOThreadPool::get()` method. It is configured in the same way as global pool with `max_io_thread_pool_size`, `max_io_thread_pool_free_size` and `io_thread_pool_queue_size` settings. The main purpose of IO thread pool is to avoid exhaustion of the global pool with IO jobs, which could prevent queries from fully utilizing CPU. Backup to S3 does significant amount of IO operations and to avoid impact on interactive queries there is a separate `BackupsIOThreadPool` configured with `max_backups_io_thread_pool_size`, `max_backups_io_thread_pool_free_size` and `backups_io_thread_pool_queue_size` settings.

For periodic task execution there is `BackgroundSchedulePool` class. You can register tasks using `BackgroundSchedulePool::TaskHolder` objects and the pool ensures that no task runs two jobs at the same time. It also allows you to postpone task execution to a specific instant in the future or temporarily deactivate task. Global `Context` provides a few instances of this class for different purposes. For general purpose tasks `Context::getSchedulePool()` is used.

There are also specialized thread pools for preemptable tasks. Such `IExecutableTask` task can be split into ordered sequence of jobs, called steps. To schedule these tasks in a manner allowing short tasks to be prioritized over long ones `MergeTreeBackgroundExecutor` is used. As name suggests it is used for background MergeTree related operations such as merges, mutations, fetches and moves. Pool instances are available using `Context::getCommonExecutor()` and other similar methods.

No matter what pool is used for a job, at start `ThreadStatus` instance is created for this job. It encapsulates all per-thread information: thread id, query id, performance counters, resource consumption and many other useful data. Job can access it via thread local pointer by `CurrentThread::get()` call, so we do not need to pass it to every function.

If thread is related to query execution, then the most important thing attached to `ThreadStatus` is query context `ContextPtr`. Every query has its master thread in the server pool. Master thread does the attachment by holding an `ThreadStatus::QueryScope query_scope(query_context)` object. Master thread also creates a thread group represented with `ThreadGroupStatus` object. Every additional thread that is allocated during this query execution is attached to its thread group by `CurrentThread::attachTo(thread_group)` call. Thread groups are used to aggregate profile event counters and track memory consumption by all threads dedicated to a single task (see `MemoryTracker` and `ProfileEvents::Counters` classes for more information).

## Concurrency control {#concurrency-control}
Query that can be parallelized uses `max_threads` setting to limit itself. Default value for this setting is selected in a way that allows single query to utilize all CPU cores in the best way. But what if there are multiple concurrent queries and each of them uses default `max_threads` setting value? Then queries will share CPU resources. OS will ensure fairness by constantly switching threads, which introduce some performance penalty. `ConcurrencyControl` helps to deal with this penalty and avoid allocating a lot of threads. Configuration setting `concurrent_threads_soft_limit_num` is used to limit how many concurrent thread can be allocated before applying some kind of CPU pressure.

:::note
`concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` are disabled (equal 0) by default. So this feature must be enabled before use.
:::

Notion of CPU `slot` is introduced. Slot is a unit of concurrency: to run a thread query has to acquire a slot in advance and release it when thread stops. The number of slots is globally limited in a server. Multiple concurrent queries are competing for CPU slots if the total demand exceeds the total number of slots. `ConcurrencyControl` is responsible to resolve this competition by doing CPU slot scheduling in a fair manner.

Each slot can be seen as an independent state machine with the following states:
 * `free`: slot is available to be allocated by any query.
 * `granted`: slot is `allocated` by specific query, but not yet acquired by any thread.
 * `acquired`: slot is `allocated` by specific query and acquired by a thread.

Note that `allocated` slot can be in two different states: `granted` and `acquired`. The former is a transitional state, that actually should be short (from the instant when a slot is allocated to a query till the moment when the up-scaling procedure is run by any thread of that query).

```mermaid
stateDiagram-v2
    direction LR
    [*] --> free
    free --> allocated: allocate
    state allocated {
        direction LR
        [*] --> granted
        granted --> acquired: acquire
        acquired --> [*]
    }
    allocated --> free: release
```

API of `ConcurrencyControl` consists of the following functions:
1. Create a resource allocation for a query: `auto slots = ConcurrencyControl::instance().allocate(1, max_threads);`. It will allocate at least 1 and at most `max_threads` slots. Note that the first slot is granted immediately, but the remaining slots may be granted later. Thus limit is soft, because every query will obtain at least one thread.
2. For every thread a slot has to be acquired from an allocation: `while (auto slot = slots->tryAcquire()) spawnThread([slot = std::move(slot)] { ... });`.
3. Update the total amount of slots: `ConcurrencyControl::setMaxConcurrency(concurrent_threads_soft_limit_num)`. Can be done in runtime, w/o server restart.

This API allows queries to start with at least one thread (in presence of CPU pressure) and later scale up to `max_threads`.

## Distributed Query Execution {#distributed-query-execution}

Servers in a cluster setup are mostly independent. You can create a `Distributed` table on one or all servers in a cluster. The `Distributed` table does not store data itself – it only provides a “view” to all local tables on multiple nodes of a cluster. When you SELECT from a `Distributed` table, it rewrites that query, chooses remote nodes according to load balancing settings, and sends the query to them. The `Distributed` table requests remote servers to process a query just up to a stage where intermediate results from different servers can be merged. Then it receives the intermediate results and merges them. The distributed table tries to distribute as much work as possible to remote servers and does not send much intermediate data over the network.

Things become more complicated when you have subqueries in IN or JOIN clauses, and each of them uses a `Distributed` table. We have different strategies for the execution of these queries.

There is no global query plan for distributed query execution. Each node has its local query plan for its part of the job. We only have simple one-pass distributed query execution: we send queries for remote nodes and then merge the results. But this is not feasible for complicated queries with high cardinality `GROUP BY`s or with a large amount of temporary data for JOIN. In such cases, we need to “reshuffle” data between servers, which requires additional coordination. ClickHouse does not support that kind of query execution, and we need to work on it.

## Merge Tree {#merge-tree}

`MergeTree` is a family of storage engines that supports indexing by primary key. The primary key can be an arbitrary tuple of columns or expressions. Data in a `MergeTree` table is stored in “parts”. Each part stores data in the primary key order, so data is ordered lexicographically by the primary key tuple. All the table columns are stored in separate `column.bin` files in these parts. The files consist of compressed blocks. Each block is usually from 64 KB to 1 MB of uncompressed data, depending on the average value size. The blocks consist of column values placed contiguously one after the other. Column values are in the same order for each column (the primary key defines the order), so when you iterate by many columns, you get values for the corresponding rows.

The primary key itself is “sparse”. It does not address every single row, but only some ranges of data. A separate `primary.idx` file has the value of the primary key for each N-th row, where N is called `index_granularity` (usually, N = 8192). Also, for each column, we have `column.mrk` files with “marks”, which are offsets to each N-th row in the data file. Each mark is a pair: the offset in the file to the beginning of the compressed block, and the offset in the decompressed block to the beginning of data. Usually, compressed blocks are aligned by marks, and the offset in the decompressed block is zero. Data for `primary.idx` always resides in memory, and data for `column.mrk` files is cached.

When we are going to read something from a part in `MergeTree`, we look at `primary.idx` data and locate ranges that could contain requested data, then look at `column.mrk` data and calculate offsets for where to start reading those ranges. Because of sparseness, excess data may be read. ClickHouse is not suitable for a high load of simple point queries, because the entire range with `index_granularity` rows must be read for each key, and the entire compressed block must be decompressed for each column. We made the index sparse because we must be able to maintain trillions of rows per single server without noticeable memory consumption for the index. Also, because the primary key is sparse, it is not unique: it cannot check the existence of the key in the table at INSERT time. You could have many rows with the same key in a table.

When you `INSERT` a bunch of data into `MergeTree`, that bunch is sorted by primary key order and forms a new part. There are background threads that periodically select some parts and merge them into a single sorted part to keep the number of parts relatively low. That’s why it is called `MergeTree`. Of course, merging leads to “write amplification”. All parts are immutable: they are only created and deleted, but not modified. When SELECT is executed, it holds a snapshot of the table (a set of parts). After merging, we also keep old parts for some time to make a recovery after failure easier, so if we see that some merged part is probably broken, we can replace it with its source parts.

`MergeTree` is not an LSM tree because it does not contain MEMTABLE and LOG: inserted data is written directly to the filesystem. This behavior makes MergeTree much more suitable to insert data in batches. Therefore frequently inserting small amounts of rows is not ideal for MergeTree. For example, a couple of rows per second is OK, but doing it a thousand times a second is not optimal for MergeTree. However, there is an async insert mode for small inserts to overcome this limitation. We did it this way for simplicity’s sake, and because we are already inserting data in batches in our applications

There are MergeTree engines that are doing additional work during background merges. Examples are `CollapsingMergeTree` and `AggregatingMergeTree`. This could be treated as special support for updates. Keep in mind that these are not real updates because users usually have no control over the time when background merges are executed, and data in a `MergeTree` table is almost always stored in more than one part, not in completely merged form.

## Replication {#replication}

Replication in ClickHouse can be configured on a per-table basis. You could have some replicated and some non-replicated tables on the same server. You could also have tables replicated in different ways, such as one table with two-factor replication and another with three-factor.

Replication is implemented in the `ReplicatedMergeTree` storage engine. The path in `ZooKeeper` is specified as a parameter for the storage engine. All tables with the same path in `ZooKeeper` become replicas of each other: they synchronize their data and maintain consistency. Replicas can be added and removed dynamically simply by creating or dropping a table.

Replication uses an asynchronous multi-master scheme. You can insert data into any replica that has a session with `ZooKeeper`, and data is replicated to all other replicas asynchronously. Because ClickHouse does not support UPDATEs, replication is conflict-free. As there is no quorum acknowledgment of inserts by default, just-inserted data might be lost if one node fails. The insert quorum can be enabled using `insert_quorum` setting.

Metadata for replication is stored in ZooKeeper. There is a replication log that lists what actions to do. Actions are: get part; merge parts; drop a partition, and so on. Each replica copies the replication log to its queue and then executes the actions from the queue. For example, on insertion, the “get the part” action is created in the log, and every replica downloads that part. Merges are coordinated between replicas to get byte-identical results. All parts are merged in the same way on all replicas. One of the leaders initiates a new merge first and writes “merge parts” actions to the log. Multiple replicas (or all) can be leaders at the same time. A replica can be prevented from becoming a leader using the `merge_tree` setting `replicated_can_become_leader`. The leaders are responsible for scheduling background merges.

Replication is physical: only compressed parts are transferred between nodes, not queries. Merges are processed on each replica independently in most cases to lower the network costs by avoiding network amplification. Large merged parts are sent over the network only in cases of significant replication lag.

Besides, each replica stores its state in ZooKeeper as the set of parts and its checksums. When the state on the local filesystem diverges from the reference state in ZooKeeper, the replica restores its consistency by downloading missing and broken parts from other replicas. When there is some unexpected or broken data in the local filesystem, ClickHouse does not remove it, but moves it to a separate directory and forgets it.

:::note
The ClickHouse cluster consists of independent shards, and each shard consists of replicas. The cluster is **not elastic**, so after adding a new shard, data is not rebalanced between shards automatically. Instead, the cluster load is supposed to be adjusted to be uneven. This implementation gives you more control, and it is ok for relatively small clusters, such as tens of nodes. But for clusters with hundreds of nodes that we are using in production, this approach becomes a significant drawback. We should implement a table engine that spans across the cluster with dynamically replicated regions that could be split and balanced between clusters automatically.
:::

[Original article](https://clickhouse.com/docs/en/development/architecture/)
