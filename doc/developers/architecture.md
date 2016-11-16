# ClickHouse quick architecture overview

> Optional side notes are in grey.


ClickHouse is a true column oriented DBMS. Data is stored by columns, and furthermore, during query execution data is processed by arrays (vectors, chunks of columns). Whenever possible, operations are dispatched not on individual values but on arrays. It is called "vectorized query execution", and it helps lower dispatch cost relative to the cost of actual data processing.

>This idea is nothing new. It dates back to the `APL` programming language and its descendants: `A+`, `J`, `K`, `Q`. Array programming is widely used in scientific data processing. Neither is this idea something new in relational databases: for example, it is used in the `Vectorwise` system.

>There are two different approaches for speeding up query processing: vectorized query execution and runtime code generation. In the latter, the code is generated for every kind of query on the fly, removing all indirection and dynamic dispatch. None of these approaches is strictly better than the other. Runtime code generation can be better when fuses many operations together, thus fully utilizing CPU execution units and pipeline. Vectorized query execution can be worse, because it must deal with temporary vectors that must be written to cache and read back. If the temporary data does not fit in L2 cache, this becomes an issue. But vectorized query execution more easily utilizes SIMD capabilities of CPU. A [research paper](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) written by our friends shows that it is better to combine both approaches. ClickHouse mostly uses vectorized query execution and has limited initial support for runtime code generation (only the inner loop of first stage of GROUP BY can be compiled).


## Columns

To represent columns in memory (in fact, chunks of columns), `IColumn` interface is used. This interface provide helper methods for implementation of various relational operators. Almost all operations are immutable: they are not modifies original column, but create new, modified one. For example, there is `IColumn::filter` method that accept filter byte mask and create new, filtered column. It is used for `WHERE` and `HAVING` relational operators. Another examples: `IColumn::permute` method to support `ORDER BY`, `IColumn::cut` method to support `LIMIT` and so on. 

Various `IColumn` implementations (`ColumnUInt8`, `ColumnString` and so on) is responsible for memory layout of columns. Memory layout is usually contiguous array. For integer type of columns it is just one contiguous array, like `std::vector`. For `String` and `Array` columns, it is two vectors: one for all array elements, placed contiguously and second is for offsets to beginning of each array. There is also `ColumnConst`, that store just one value in memory, but looks like column.


## Field

Nevertheless, it is possible to work with individual values too. To represent individual value, `Field` is used. `Field` is just a discriminated union of `UInt64`, `Int64`, `Float64`, `String` and `Array`. `IColumn` has method `operator[]` to get n-th value as a `Field`, and `insert` method to append a `Field` to end of a column. These methods are not very efficient, because they require to deal with temporary `Field` object, representing individual value. There are more efficient methods, for example: `insertFrom`, `insertRangeFrom` and so on.

`Field` has not enough information about specific data type for table. For example, `UInt8`, `UInt16`, `UInt32`, `UInt64` all represented as `UInt64` in a `Field`.


### Leaky abstractions

`IColumn` has methods for common relational transformations of data. But not for all needs. For example, `ColumnUInt64` has no method to calculate sum of two columns, or `ColumnString` has no method to run substring search. These countless routines are implemented outside of `IColumn`.

Various functions on columns could be implemented in generic, non-efficient way, using methods of `IColumn` to extract `Field` values, or in specialized way, using knowledge of inner memory layout of data in concrete `IColumn` implementation. To do this, functions are casting to concrete `IColumn` type and deal with internal representation directly. For example, `ColumnUInt64` has `getData` method that return reference to internal array; and then separate routine could read or fill that array directly. In fact, we have "leaky abstractions" to allow efficient specializations of various routines.


## Data Types

`IDataType` is responsible for serialization and deserialization: for reading and writing chunks of columns or individual values in binary or text form.
`IDataType` is directly corresponding to data types in tables. For example, there are `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` and so on.

`IDataType` and `IColumn` are only loosely related to each other. Different data types could be represented in memory by same `IColumn` implementations. For example, `DataTypeUInt32` and `DataTypeDateTime` are represented both with `ColumnUInt32` or `ColumnConstUInt32`. Also, same data type could be represented by different `IColumn` implementations. For example. `DataTypeUInt8` could be represented with `ColumnUInt8` or `ColumnConstUInt8`.

`IDataType` only store metadata. For example, `DataTypeUInt8` doesn't store anything at all (except vptr) and `DataTypeFixedString` stores just `N` - size of fixed-size strings.

`IDataType` has helper methods for various data formats. For example, it has methods to serialize value with possible quoting; to serialize value suitable to be placed in JSON; to serialize value as part of XML format. There is no direct correspondence with data formats. For example, different data formats: `Pretty`, `TabSeparated` could use one helper method `serializeTextEscaped` from `IDataType` interface.


## Block

`Block` is a container to represent subset (chunk) of table in memory. It is just set of triples: `(IColumn, IDataType, column name)`. During query execution, data is processed by `Block`s. Having `Block`, we have data (in `IColumn` object), we have information about its type (in `IDataType`) - how to deal with that column, and we have column name (some name, either original name of column from table, or some artificial name, assigned for temporary results of calculations).

When we calculate some function over columns in a block, we add another column with its result to a block, and don't touch columns for arguments of function: operations are immutable. Later, unneeded columns could be removed from block, but not modified. It is convenient to allow common subexpressions elimination.

>Blocks are created for every processed chunk of data. Worth to note that in one type of calculation, column names and types remaining the same for different blocks, only column data changes. It will be better to split block data and block header, because for small block sizes, we will have high overhead of temporary string for column names and shared_ptrs copying.


## Block Streams

To process data: to read data from somewhere, to do transformations on data, or to write data somewhere, we have streams of blocks. `IBlockInputStream` has method `read`: to fetch next block while available. `IBlockOutputStream` has method `write`: to push block somewhere.

Streams are responsible for various things:

1. To read or write to a table. Table just returns a stream, from where you could read or where to write blocks.

2. To implement data formats. For example, if you want to output data to a terminal in `Pretty` format, you create block output stream, where you push blocks, and it will format them.

3. To do transformations on data. Let you have a `IBlockInputStream` and want to create filtered stream. You create `FilterBlockInputStream` and initialize it with your stream. Then when you pull a block from `FilterBlockInputStream`, it will pull a block from your stream, do filtering and return filtered block to you. Query execution pipelines are represented that way.

There are more sophisticated transformations. For example, when you pull from `AggregatingBlockInputStream`, it will read all data from its source, aggregate it, and then return stream of aggregated data for you. Another example: `UnionBlockInputStream` will accept many input sources in constructor and also a number of threads. It will launch many threads and read from many sources in parallel.

> Block streams are using "pull" approach to control flow: when you pull a block from first stream, it will consequently pull required blocks from nested streams, and all execution pipeline will work. Neither "pull" nor "push" is the best solution, because control flow is implicit, and that limits implementation of various features like simultaneous execution of multiple queries (merging many pipelines together). That limitation could be overcomed with coroutines or just running extra threads that wait for each other. We may have more possibilities if we made control flow explicit: if we locate logic for passing data from one calculation unit to another outside of that calculation units. Read [nice article](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/) for more thoughts.

> Worth to note that query execution pipeline creates temporary data on each step. We are trying to keep block size small enough for that temporary data to fit in CPU cache. With that assumption, writing and reading temporary data is almost free comparable to other calculations. We could consider an alternative: to fuse many operations in pipeline together, to make pipeline as short as possible and remove much of temporary data. It could be an advantage, but there are also drawbacks. For example, in splitted pipeline it is easy to implement caching of intermediate data, stealing of intermediate data from similar queries running at the same time, and merging pipelines for similar queries.


## Formats

Data formats are implemented with block streams. There are "presentational" formats, only suitable for output of data to the client: for example, `Pretty` format: it provides only `IBlockOutputStream`. And there are input/output formats, example: `TabSeparated` or `JSONEachRow`.

There are also row streams: `IRowInputStream`, `IRowOutputStream`. They allow to pull/push data by individual rows, not by blocks. And they are only needed to simplify implementation of row oriented formats. There are wrappers `BlockInputStreamFromRowInputStream`, `BlockOutputStreamFromRowOutputStream` to convert row oriented streams to usual block oriented streams.


## IO

For byte oriented input/output, there are `ReadBuffer` and `WriteBuffer` abstract classes. They are used instead of C++ `iostream`s. Don't worry: every mature C++ project is using something instead of `iostream`s for good reasons.

`ReadBuffer` and `WriteBuffer` is just a contiguous buffer and a cursor pointing to position in that buffer. Implementations could do or do not own the memory for buffer. There is a virtual method to fill buffer with next data (in case or `ReadBuffer`) or to flush buffer somewhere (in case of `WriteBuffer`). Virtual methods are only rarely called.

Implementations of `ReadBuffer`/`WriteBuffer` are used to work with files and file descriptors, network sockets; to implement compression (`CompressedWriteBuffer` is initialized with another WriteBuffer and do compression before writing data to it); names `ConcatReadBuffer`, `LimitReadBuffer`, `HashingWriteBuffer` speak for themself.

Read/WriteBuffers only deal with bytes. To help with formatted input/output (example: write a number in decimal format), there are functions from `ReadHelpers` and `WriteHelpers` header files.

Lets look what happens when you want to write resultset in `JSON` format to stdout. You have result set, ready to be fetched from `IBlockInputStream`. You create `WriteBufferFromFileDescriptor(STDOUT_FILENO)` to write bytes to stdout. You create `JSONRowOutputStream`, initialized with that `WriteBuffer`, to write rows in `JSON` to stdout. You create `BlockOutputStreamFromRowOutputStream` on top of it, to represent it as `IBlockOutputStream`. Then you call `copyData` to transfer data from `IBlockInputStream` to `IBlockOutputStream`, and all works. Internally, `JSONRowOutputStream` will write various JSON delimiters and call method `IDataType::serializeTextJSON` with reference to `IColumn` and row number as arguments. Consequently, `IDataType::serializeTextJSON` will call a method from `WriteHelpers.h`: for example, `writeText` for numeric types and `writeJSONString` for `DataTypeString`.


## Tables

Tables are represented with `IStorage` interface. Different implementations of that interface are different table engines. Example: `StorageMergeTree`, `StorageMemory` and so on. Instances of that classes are just tables.

Most important methods of `IStorage` are `read` and `write`. There are also `alter`, `rename`, `drop` and so on. `read` method accepts following arguments: set of columns to read from a table, query `AST` to consider, desired number of streams to return. And it returns: one or multiple of `IBlockInputStream` objects and information about to what stage data was already processed inside a table engine during query execution.

In most cases, method read is responsible only for reading specified columns from a table. Not for any further data processing. All further data processing are done by query interpreter - outside of responsibility of `IStorage`.

But there are notable exceptions:
- query AST is passed to `read` method and table engine could consider it to derive index usage and to read fewer data from a table;
- sometimes, table engine could process data itself to specific stage: for example, `StorageDistributed` will send query to remote servers, ask them to process data to a stage, where data from different remote servers could be merged, and return that preprocessed data.
Data will be completely processed to final result using query interpreter.

`read` method of table could return many `IBlockInputStream` objects - to allow parallel data processing. That multiple block input streams could read from a table in parallel. Then you could wrap that streams with various transformations (expression evaluation, filtering) that could be calculated independently and create `UnionBlockInputStream` on top of them, to read from multiple streams in parallel.

There are also `TableFunction`s. That are functions that returns temporary `IStorage` object, to use in `FROM` clause of a query.

To get quick understanding, how to implement your own table engine - look at something simple, like `StorageMemory` or `StorageTinyLog`.

> As result of `read` method, `IStorage` will return `QueryProcessingStage` - information about what parts of query was already calculated inside a storage. Currently we have only very coarse granularity of that information. There is no way for storage to say "I have already processed that part of expression in WHERE, for that ranges of data". Need to work on that.


## Parsers

Query is parsed by hand-written recursive descent parser. For example, `ParserSelectQuery` just recursively calls underlying parsers for various parts of query. Parsers create an `AST`. `AST` is represented by nodes - instances of `IAST`.

> Parser generators are not used for historical reason.


## Interpreters

Interpreters are responsible for creating query execution pipeline from an `AST`. There are simple interpreters, for example, `InterpreterExistsQuery`, `InterpreterDropQuery` or more sophisticated, `InterpreterSelectQuery`. Query execution pipeline is a composition of block input or output streams. For example, result of interpretation of `SELECT` query is `IBlockInputStream` from where to read resultset; result of INSERT query is `IBlockOutputStream` where to write a data for insertion; and result of interpretation of `INSERT SELECT` query is an `IBlockInputStream`, that returns empty resultset on first read, but copy data from `SELECT` to `INSERT` in same time.

`InterpreterSelectQuery` uses `ExpressionAnalyzer` and `ExpressionActions` machinery to do analyze and transformations of query. Most of rule based query optimizations are done in that place. `ExpressionAnalyzer` is quite messy and should be rewritten: various query transformations and optimizations should be extracted to separate classes to allow modular transformations or query.


## Functions

There are ordinary functions and aggregate functions. For aggregate functions, see next section.

Ordinary functions doesn't change number of rows - they work as if they processing each row independently of each other. In fact, functions are called not for individual rows, but for `Block`s of data, to implement vectorized query execution.

There are some miscellaneous functions, like `blockSize`, `rowNumberInBlock`, `runningAccumulate`, that exploit block processing and violates independence of rows.

ClickHouse has strong typing: no implicit type conversion happens. If a function doesn't support specific combination of types, an exception will be thrown. But functions could work (be overloaded) for many different combinations of types. For example, function `plus` (to implement operator `+`) works for any combination of numeric types: `UInt8` + `Float32`, `UInt16` + `Int8` and so on. Also, some functions could accept variadic number of arguments, for example, function `concat`.

Implementing a function could be slightly unconvenient, because a function is doing explicit dispatching on supported data types, and on supported `IColumns`. For example, function `plus` has code, generated by instantination of C++ template, for each combination of numeric types, and for constant or non-constant left and right arguments.

> Nice place to implement runtime code generation to avoid template code bloat. Also it will make possible to add fused functions, like fused multiply-add or to do few comparisons in one loop iteration.

> Due to vectorized query execution, functions are not short-curcuit. For example, if you write `WHERE f(x) AND g(y)`, both sides will be calculated, even for rows, when `f(x)` is zero (except the case when `f(x)` is zero constant expression). But if selectivity of `f(x)` condition is high, and calculation of `f(x)` is much cheaper than `g(y)`, it will be better to implement multi-pass calculation: first calculate `f(x)`, then filter columns by its result, and then calculate `g(y)` only for smaller, filtered chunks of data.


## Aggregate Functions

Aggregate functions is a stateful functions. They accumulate passed values into some state, and finally allow to get result from that state. They are managed with `IAggregateFunction` interface. States could be rather simple (for example, state for `AggregateFunctionCount` is just single `UInt64` value) or quite complex (example: state of `AggregateFunctionUniqCombined` is a combination of linear array, hash table and `HyperLogLog` probabilistic data structure).

To deal with many states while executing high-cardinality `GROUP BY` query, states are allocated in `Arena` - a memory pool, or they could be allocated in any suitable piece of memory. States could have non-trivial constructor and destructor: for example, complex aggregation states could allocate additional memory by themself. It requires some attention to create and destroy states and to proper pass their ownership: to mind, who and when will destroy states.

Aggregation states could be serialized and deserialized to pass it over network during distributed query execution or to write them on disk where there is not enough RAM. Or even, they could be stored in a table, with `DataTypeAggregateFunction`, to allow incremental aggregation of data.

> Serialized data format for aggregate functions states is not versioned right now. It is Ok if aggregate states are only stored temporarily. But we have `AggregatingMergeTree` table engine for incremental aggregation, and people already using it in production. That's why we should add support for backward compatibility at next change of serialized format for any aggregate function.


## Server

Server implements few different interfaces:
- HTTP interface for any foreign clients;
- TCP interface for native clickhouse-client and for cross-server communication during distributed query execution;
- interface for transferring data for replication.

Internally it is just simple multithreaded server. No coroutines, fibers, etc. Because server is not intended to process high rate of simple queries, but to process relatively low rate of hard queries, each of them could process vast amount of data for analytics.

Server initializes `Context` class with necessary environment for query execution: list of available databases, users and access rights, settings, clusters, process list, query log and so on. That environment is used by interpreters.

We maintain full backward and forward compatibility for server TCP protocol: old clients could talk to new servers and new clients could talk to old servers. But we don't want to maintain it eternally and we are removing support for old versions after about one year.

> For all external applications, it is recommended to use HTTP interface, because it is simple and easy to use. TCP protocol is more tight to internal data structures: it uses internal format for passing blocks of data and it uses custom framing for compressed data. We don't release C library for that protocol, because it requires linking most of ClickHouse codebase, which is not practical.


## Distributed query execution

Servers in a cluster setup are mostly independent. You could create a `Distributed` table on one or all servers in a cluster. `Distributed` table does not store data itself, it only provides a "view" to all local tables on multiple nodes of a cluster. When you SELECT from a `Distributed` table, it will rewrite that query, choose remote nodes according to load balancing settings and send query to them. `Distributed` table requests for remote servers to process query not up to final result, but to a stage where intermediate results from different servers could be merged. Then it receives intermediate results and merge them. Distributed table is trying to distribute as much work as possible to remote servers, and do not send much intermediate data over network.

> Things become more complicated when you have subqueries in IN or JOIN clause, each of them uses `Distributed` table. We have different strategies for execution of that queries.

> There is no global query plan for distributed query execution. Each node has its own local query plan for its part of job. We have only simple one-pass  distributed query execution: we send queries for remote nodes and then merge its results. But it is not feasible for difficult queries with high cardinality GROUP BYs or with large amount of temporary data for JOIN: in that cases we need to do "reshuffle" data between servers, that requires additional coordination. ClickHouse does not support that kind of query execution and we need to work on it.


## Merge Tree

`MergeTree` is a family of storage engines, that support index by primary key. Primary key could be arbitary tuple of columns or expressions. Data in `MergeTree` table is stored in "parts". Each part stores data in primary key order (data is ordered lexicographically by primary key tuple). All columns of table are stored in separate `column.bin` files in that parts. That files consists of compressed blocks. Each block is usually from 64 KB to 1 MB of uncompressed data, depending of average size of value. That blocks consists of column values, placed contiguously one after the other. Column values are in same order (defined by primary key) for each column - so, when you iterate by many columns, you get values for corresponding rows.

Primary key itself is "sparse". It could address not each single row, but only some ranges of data. In separate `primary.idx` file we have value of primary key for each N-th row, where N is called `index_granularity`. Usually N = 8192. Also, for each column, we have `column.mrk` files with "marks": offsets to each N-th row in data file. Each mark is a pair: offset in file to begin of compressed block and offset in decompressed block to begin of data. Usually compressed blocks are aligned by marks, and offset in decompressed block is zero. Data for `primary.idx` is always reside in memory and data for `column.mrk` files are cached.

When we are going to read something from part in `MergeTree`, we look at `primary.idx` data, locate ranges, that could possibly contain requested data, then look at `column.mrk` data and calculate offsets, from where to start read that ranges. Because of sparseness, excess data could be read. ClickHouse is not suitable for high load of simple point queries, because for each key, whole range with `index_granularity` rows must be read, and whole compressed block must be decompressed, for each column. We made index sparse, because we must be able to maintain trillions of rows per single server without noticeably memory consumption for index. Also, because primary key is sparse, it is not unique: it cannot check existence of key in table at INSERT time. You could have many rows with same key in a table.

When you `INSERT` bunch of data into `MergeTree`, that bunch is sorted by primary key order and forms a new part. To keep number of parts relatively low, there is background threads, that periodically select some parts and merge it to single sorted part. That's why it is called `MergeTree`. Of course, merging leads to "write amplification". All parts are immutable: they are only created and deleted, but not modified. When SELECT is run, it holds a snapshot (set of parts) of table. Also, after merging, we keep old parts for some time to make recovery after failure easier: if we see that some merged part is probably broken, we could replace it with its source parts.

`MergeTree` is not a LSM-Tree because it doesn't contain "memtable" and "log": inserted data are written directly to filesystem. That makes it suitable only to INSERT data in batches, not by one row and not very frequently: about each second is Ok, but thousand times a second is not. We done it that way for simplicity reasons and because we are already inserting data in batches in our applications.

> MergeTree tables could only have one (primary) index: there is no secondary indices. It would be nice to allow multiple of physical representations under one logical table: for example, to store data in more than one physical order or even to allow representations with pre-aggregated data along with original data.

> There is MergeTree engines, that doing additional work while background merges. Examples are `CollapsingMergeTree`, `AggregatingMergeTree`. This could be treat as special support for updates. Remind that it is not real updates because user usually have no control over moment of time when background merges will be executed, and data in `MergeTree` table is almost always stored in more than one part - not in completely merged form.


## Replication

Replication in ClickHouse is implemented on per-table basis. You could have some replicated and some non-replicated tables on single server. And you could have tables replicated in different ways: for example, one table with two-factor replication and another with three-factor.

Replication is implemented in `ReplicatedMergeTree` storage engine. As a parameter for storage engine, path in `ZooKeeper` is specified. All tables with same path in `ZooKeeper` becomes replicas of each other: they will synchronise its data and maintain consistency. Replicas could be added and removed dynamically: simply by creation or dropping a table.

Replication works in asynchronous multi-master scheme. You could insert data into any replica which have a session with `ZooKeeper`, and data will be replicated to all other replicas asynchronously. Because ClickHouse doesn't support UPDATEs, replication is conflict-free. As there is no quorum acknowledgment of inserts, just-inserted data could be lost in case of one node failure.

Metadata for replication is stored in ZooKeeper. There is replication log, that consists of what actions to do. Actions are: get part; merge parts; drop partition, etc. All replicas copy replication log to its queue and then execute actions from queue. For example, on insertion, "get part" action is created in log, and every replica will download that part. Merges are coordinated between replicas to get byte-identical result. All parts are merged in same way on all replicas. To achieve that, one replica is elected as leader, and that replica will initiate merges and write "merge parts" actions to log.

Replication is physical: only compressed parts are transferred between nodes, not queries. To lower network cost (to avoid network amplification), merges are processed on each replica independently in most cases. Large merged parts are sent over network only in case of significant replication lag.

Also each replica keeps their state in ZooKeeper: set of parts and its checksums. When state on local filesystem is diverged from reference state in ZooKeeper, replica will restore its consistency by downloading missing and broken parts from other replicas. When there is some unexpected or broken data in local filesystem, ClickHouse will not remove it, but move to separate directory and forget.

> ClickHouse cluster consists of independent shards and each shard consists of replicas. Cluster is not elastic: after adding new shard, data is not rebalanced between shards automatically. Instead, cluster load will be uneven. This implementation gives you more control and it is fine for relatively small clusters: tens of nodes. But for hundreds of nodes clusters, that we are using in production, this approach becomes significant drawback. We should implement table engine that will span its data across cluster with dynamically replicated regions, that could be splitted and balanced between cluster automatically.
