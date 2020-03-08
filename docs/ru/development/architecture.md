# Обзор архитектуры ClickHouse

ClickHouse - полноценная колоночная СУБД. Данные хранятся в колонках, а в процессе обработки - в массивах (векторах или фрагментах (chunk’ах) колонок). По возможности операции выполняются на массивах, а не на индивидуальных значениях. Это называется “выполнение векторизованного запроса” (vectorized query execution), и помогает снизить стоимость обработки данных.

> Эта идея не нова. Такой подход использовался в `APL` (A programming language, 1957) и его потомках: `A +` (диалект `APL`), `J` (1990), `K` (1993) и `Q` (язык программирвоания Kx Systems, 2003). Программирование на массивах (Array programming) используется в научных вычислительных системах. Эта идея не является чем-то новым и для реляционных баз данных: например, она используется в системе `VectorWise` (так же известной как Actian Vector Analytic Database от Actian Corporation).

Существует два различных подхода для увеличения скорости обработки запросов: выполнение векторизованного запроса и генерация кода во время выполнения (runtime code generation). В последнем случае код генерируется на лету для каждого типа запроса, удаляя все косвенные и динамические обращения. Ни один из этих подходов не имеет явного преимущества. Генерация кода во время выполнения выигрывает, если объединяет большое число операций, таким образом полностью используя вычислительные блоки и конвейер CPU. Выполнение векторизованного запроса может быть менее практично потому, что задействует временные векторы данных, которые должны быть записаны и прочитаны из кэша. Если временные данные не помещаются в L2 кэш, будут проблемы. С другой стороны выполнение векторизованного запроса упрощает использование SIMD инструкций CPU. [Научная работа](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) наших друзей показывает преимущества сочетания обоих подходов. ClickHouse использует выполнение векторизованного запроса и имеет ограниченную начальную поддержку генерации кода во время выполнения.

## Колонки

Для представления столбцов в памяти (фактически, фрагментов столбцов) используется интерфейс `IColumn`. Интерфейс предоставляет вспомогательные методы для реализации различных реляционных операторов. Почти все операции иммутабельные: они не изменяют оригинальных колонок, а создают новую с измененными значениями. Например, метод `IColumn :: filter` принимает фильтр - набор байт. Он используется для реляционных операторов `WHERE` и `HAVING`. Другой пример: метод `IColumn :: permute` используется для поддержки `ORDER BY`, метод `IColumn :: cut` - `LIMIT` и т. д.

Различные реализации `IColumn` (`ColumnUInt8`, `ColumnString` и т. д.) отвечают за распределение данных колонки в памяти. Для колонок целочисленного типа это один смежный массив, такой как `std :: vector`. Для колонок типа `String` и `Array` это два вектора: один для всех элементов массивов, располагающихся смежно, второй для хранения смещения до начала каждого массива. Также существует реализация `ColumnConst`, в которой хранится только одно значение в памяти, но выглядит как колонка.

## Поля

Тем не менее, можно работать и с индивидуальными значениями. Для представления индивидуальных значений используется `Поле` (`Field`). `Field` - размеченное объединение `UInt64`, `Int64`, `Float64`, `String` и `Array`. `IColumn` имеет метод `оператор []` для получения значения по индексу n как `Field`, а также метод insert для добавления `Field` в конец колонки. Эти методы не очень эффективны, так как требуют временных объектов `Field`, представляющих индивидуальное значение. Есть более эффективные методы, такие как `insertFrom`, `insertRangeFrom` и т.д.

`Field` не несет в себе достаточно информации о конкретном типе данных в таблице. Например, `UInt8`, `UInt16`, `UInt32` и `UInt64` в `Field` представлены как `UInt64`.

## Дырявые абстракции (Leaky Abstractions)

`IColumn` предоставляет методы для общих реляционных преобразований данных, но они не отвечают всем потребностям. Например, `ColumnUInt64` не имеет метода для вычисления суммы двух столбцов, а `ColumnString` не имеет метода для запуска поиска по подстроке. Эти бесчисленные процедуры реализованы вне `IColumn`.

Различные функции на колонках могут быть реализованы обобщенным, неэффективным путем, используя `IColumn` методы для извлечения значений `Field`, или специальным путем, используя знания о внутреннем распределение данных в памяти в конкретной реализации `IColumn`. Для этого функции приводятся к конкретному типу `IColumn` и работают напрямую с его внутренним представлением. Например, в `ColumnUInt64` есть метод getData, который возвращает ссылку на внутренний массив, чтение и заполнение которого, выполняется отдельной процедурой напрямую. Фактически, мы имеем дырявую абстракции, обеспечивающие эффективные специализации различных процедур.

## Типы данных (Data Types)

`IDataType` отвечает за сериализацию и десериализацию - чтение и запись фрагментов колонок или индивидуальных значений в бинарном или текстовом формате.
`IDataType` точно соответствует типам данных в таблицах - `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` и т. д.

`IDataType` и `IColumn` слабо связаны друг с другом. Различные типы данных могут быть представлены в памяти с помощью одной реализации `IColumn`. Например, и `DataTypeUInt32`, и `DataTypeDateTime` в памяти представлены как `ColumnUInt32` или `ColumnConstUInt32`. В добавок к этому, один тип данных может быть представлен различными реализациями `IColumn`. Например, `DataTypeUInt8` может быть представлен как `ColumnUInt8` и `ColumnConstUInt8`.

`IDataType` хранит только метаданные. Например, `DataTypeUInt8` не хранить ничего (кроме скрытого указателя `vptr`), а `DataTypeFixedString` хранит только `N` (фиксированный размер строки).

В `IDataType` есть вспомогательные методы для данных различного формата. Среди них методы сериализации значений, допускающих использование кавычек, сериализации значения в JSON или XML. Среди них нет прямого соответствия форматам данных. Например, различные форматы `Pretty` и `TabSeparated` могут использовать один вспомогательный метод `serializeTextEscaped` интерфейса `IDataType`.

## Блоки (Block)

`Block` это контейнер, который представляет фрагмент (chunk) таблицы в памяти. Это набор троек - `(IColumn, IDataType, имя колонки)`. В процессе выполнения запроса, данные обрабатываются `Block`ами. Если у нас есть `Block`, значит у нас есть данные (в объекте `IColumn`), информация о типе (в `IDataType`), которая говорит нам, как работать с колонкой, и имя колонки (оригинальное имя колонки таблицы или служебное имя, присвоенное для получения промежуточных результатов вычислений).

При вычислении некоторой функции на колонках в блоке мы добавляем еще одну колонку с результатами в блок, не трогая колонки аргументов функции, потому что операции иммутабельные. Позже ненужные колонки могут быть удалены из блока, но не модифицированы. Это удобно для устранения общих подвыражений.

Блоки создаются для всех обработанных фрагментов данных. Напоминаем, что одни и те же типы вычислений, имена колонок и типы переиспользуются в разных блоках и только данные колонок изменяются. Лучше разделить данные и заголовок блока потому, что в блоках маленького размера мы имеем большой оверхэд по временным строкам при копировании умных указателей (`shared_ptrs`) и имен колонок.

## Потоки блоков (Block Streams)

Потоки блоков обрабатывают данные. Мы используем потоки блоков для чтения данных, трансформации или записи данных куда-либо. `IBlockInputStream` предоставляет метод `read` для получения следующего блока, пока это возможно, и метод `write`, чтобы продвигать (push) блок куда-либо.

Потоки отвечают за:

1. Чтение и запись в таблицу. Таблица лишь возвращает поток для чтения или записи блоков.
2. Реализацию форматов данных. Например, при выводе данных в терминал в формате `Pretty`, вы создаете выходной поток блоков, который форматирует поступающие в него блоки.
3. Трансформацию данных. Допустим, у вас есть `IBlockInputStream` и вы хотите создать отфильтрованный поток. Вы создаете `FilterBlockInputStream` и инициализируете его вашим потоком. Затем вы тянете (pull) блоки из `FilterBlockInputStream`, а он тянет блоки исходного потока, фильтрует их и возвращает отфильтрованные блоки вам. Таким образом построены конвееры выполнения запросов.

Имеются и более сложные трансофрмации. Например, когда вы тянете блоки из `AggregatingBlockInputStream`, он читает все данные своего источника, агрегирует их и возвращает поток агрегированных данных вам. Другой пример: конструктор `UnionBlockInputStream` принимает множество источников входных данных и число потоков. Такой `Stream` работает в несколько потоков и читает данные источников параллельно. 

> Block streams use the "pull" approach to control flow: when you pull a block from the first stream, it consequently pulls the required blocks from nested streams, and the entire execution pipeline will work. Neither "pull" nor "push" is the best solution, because control flow is implicit, and that limits implementation of various features like simultaneous execution of multiple queries (merging many pipelines together). This limitation could be overcome with coroutines or just running extra threads that wait for each other. We may have more possibilities if we make control flow explicit: if we locate the logic for passing data from one calculation unit to another outside of those calculation units. Read this [article](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/) for more thoughts.

We should note that the query execution pipeline creates temporary data at each step. We try to keep block size small enough so that temporary data fits in the CPU cache. With that assumption, writing and reading temporary data is almost free in comparison with other calculations. We could consider an alternative, which is to fuse many operations in the pipeline together, to make the pipeline as short as possible and remove much of the temporary data. This could be an advantage, but it also has drawbacks. For example, a split pipeline makes it easy to implement caching intermediate data, stealing intermediate data from similar queries running at the same time, and merging pipelines for similar queries.

## Formats

Data formats are implemented with block streams. There are "presentational" formats only suitable for output of data to the client, such as `Pretty` format, which provides only `IBlockOutputStream`. And there are input/output formats, such as `TabSeparated` or `JSONEachRow`.

There are also row streams: `IRowInputStream` and `IRowOutputStream`. They allow you to pull/push data by individual rows, not by blocks. And they are only needed to simplify implementation of row-oriented formats. The wrappers `BlockInputStreamFromRowInputStream` and `BlockOutputStreamFromRowOutputStream` allow you to convert row-oriented streams to regular block-oriented streams.

## I/O

For byte-oriented input/output, there are `ReadBuffer` and `WriteBuffer` abstract classes. They are used instead of C++ `iostream`s. Don't worry: every mature C++ project is using something other than `iostream`s for good reasons.

`ReadBuffer` and `WriteBuffer` are just a contiguous buffer and a cursor pointing to the position in that buffer. Implementations may own or not own the memory for the buffer. There is a virtual method to fill the buffer with the following data (for `ReadBuffer`) or to flush the buffer somewhere (for `WriteBuffer`). The virtual methods are rarely called.

Implementations of `ReadBuffer`/`WriteBuffer` are used for working with files and file descriptors and network sockets, for implementing compression (`CompressedWriteBuffer` is initialized with another WriteBuffer and performs compression before writing data to it), and for other purposes – the names `ConcatReadBuffer`, `LimitReadBuffer`, and `HashingWriteBuffer` speak for themselves.

Read/WriteBuffers only deal with bytes. To help with formatted input/output (for instance, to write a number in decimal format), there are functions from `ReadHelpers` and `WriteHelpers` header files.

Let's look at what happens when you want to write a result set in `JSON` format to stdout. You have a result set ready to be fetched from `IBlockInputStream`. You create `WriteBufferFromFileDescriptor(STDOUT_FILENO)` to write bytes to stdout. You create `JSONRowOutputStream`, initialized with that `WriteBuffer`, to write rows in `JSON` to stdout. You create `BlockOutputStreamFromRowOutputStream` on top of it, to represent it as `IBlockOutputStream`. Then you call `copyData` to transfer data from `IBlockInputStream` to `IBlockOutputStream`, and everything works. Internally, `JSONRowOutputStream` will write various JSON delimiters and call the `IDataType::serializeTextJSON` method with a reference to `IColumn` and the row number as arguments. Consequently, `IDataType::serializeTextJSON` will call a method from `WriteHelpers.h`: for example, `writeText` for numeric types and `writeJSONString` for `DataTypeString`.

## Tables

Tables are represented by the `IStorage` interface. Different implementations of that interface are different table engines. Examples are `StorageMergeTree`, `StorageMemory`, and so on. Instances of these classes are just tables.

The most important `IStorage` methods are `read` and `write`. There are also `alter`, `rename`, `drop`, and so on. The `read` method accepts the following arguments: the set of columns to read from a table, the `AST` query to consider, and the desired number of streams to return. It returns one or multiple `IBlockInputStream` objects and information about the stage of data processing that was completed inside a table engine during query execution.

In most cases, the read method is only responsible for reading the specified columns from a table, not for any further data processing. All further data processing is done by the query interpreter and is outside the responsibility of `IStorage`.

But there are notable exceptions:

- The AST query is passed to the `read` method and the table engine can use it to derive index usage and to read less data from a table.
- Sometimes the table engine can process data itself to a specific stage. For example, `StorageDistributed` can send a query to remote servers, ask them to process data to a stage where data from different remote servers can be merged, and return that preprocessed data.
The query interpreter then finishes processing the data.

The table's `read` method can return multiple `IBlockInputStream` objects to allow parallel data processing. These multiple block input streams can read from a table in parallel. Then you can wrap these streams with various transformations (such as expression evaluation or filtering) that can be calculated independently and create a `UnionBlockInputStream` on top of them, to read from multiple streams in parallel.

There are also `TableFunction`s. These are functions that return a temporary `IStorage` object to use in the `FROM` clause of a query.

To get a quick idea of how to implement your own table engine, look at something simple, like `StorageMemory` or `StorageTinyLog`.

> As the result of the `read` method, `IStorage` returns `QueryProcessingStage` – information about what parts of the query were already calculated inside storage. Currently we have only very coarse granularity for that information. There is no way for the storage to say "I have already processed this part of the expression in WHERE, for this range of data". We need to work on that.

## Parsers

A query is parsed by a hand-written recursive descent parser. For example, `ParserSelectQuery` just recursively calls the underlying parsers for various parts of the query. Parsers create an `AST`. The `AST` is represented by nodes, which are instances of `IAST`.

> Parser generators are not used for historical reasons.

## Interpreters

Interpreters are responsible for creating the query execution pipeline from an `AST`. There are simple interpreters, such as `InterpreterExistsQuery`and `InterpreterDropQuery`, or the more sophisticated `InterpreterSelectQuery`. The query execution pipeline is a combination of block input or output streams. For example, the result of interpreting the `SELECT` query is the `IBlockInputStream` to read the result set from; the result of the INSERT query is the `IBlockOutputStream` to write data for insertion to; and the result of interpreting the `INSERT SELECT` query is the `IBlockInputStream` that returns an empty result set on the first read, but that copies data from `SELECT` to `INSERT` at the same time.

`InterpreterSelectQuery` uses `ExpressionAnalyzer` and `ExpressionActions` machinery for query analysis and transformations. This is where most rule-based query optimizations are done. `ExpressionAnalyzer` is quite messy and should be rewritten: various query transformations and optimizations should be extracted to separate classes to allow modular transformations or query.

## Functions

There are ordinary functions and aggregate functions. For aggregate functions, see the next section.

Ordinary functions don't change the number of rows – they work as if they are processing each row independently. In fact, functions are not called for individual rows, but for `Block`'s of data to implement vectorized query execution.

There are some miscellaneous functions, like [blockSize](../query_language/functions/other_functions.md#function-blocksize), [rowNumberInBlock](../query_language/functions/other_functions.md#function-rownumberinblock), and [runningAccumulate](../query_language/functions/other_functions.md#function-runningaccumulate), that exploit block processing and violate the independence of rows.

ClickHouse has strong typing, so implicit type conversion doesn't occur. If a function doesn't support a specific combination of types, an exception will be thrown. But functions can work (be overloaded) for many different combinations of types. For example, the `plus` function (to implement the `+` operator) works for any combination of numeric types: `UInt8` + `Float32`, `UInt16` + `Int8`, and so on. Also, some variadic functions can accept any number of arguments, such as the `concat` function.

Implementing a function may be slightly inconvenient because a function explicitly dispatches supported data types and supported `IColumns`. For example, the `plus` function has code generated by instantiation of a C++ template for each combination of numeric types, and for constant or non-constant left and right arguments.

> This is a nice place to implement runtime code generation to avoid template code bloat. Also, it will make it possible to add fused functions like fused multiply-add, or to make multiple comparisons in one loop iteration.

Due to vectorized query execution, functions are not short-circuit. For example, if you write `WHERE f(x) AND g(y)`, both sides will be calculated, even for rows, when `f(x)` is zero (except when `f(x)` is a zero constant expression). But if selectivity of the `f(x)` condition is high, and calculation of `f(x)` is much cheaper than `g(y)`, it's better to implement multi-pass calculation: first calculate `f(x)`, then filter columns by the result, and then calculate `g(y)` only for smaller, filtered chunks of data.

## Aggregate Functions

Aggregate functions are stateful functions. They accumulate passed values into some state, and allow you to get results from that state. They are managed with the `IAggregateFunction` interface. States can be rather simple (the state for `AggregateFunctionCount` is just a single `UInt64` value) or quite complex (the state of `AggregateFunctionUniqCombined` is a combination of a linear array, a hash table and a `HyperLogLog` probabilistic data structure).

To deal with multiple states while executing a high-cardinality `GROUP BY` query, states are allocated in `Arena` (a memory pool), or they could be allocated in any suitable piece of memory. States can have a non-trivial constructor and destructor: for example, complex aggregation states can allocate additional memory themselves. This requires some attention to creating and destroying states and properly passing their ownership, to keep track of who and when will destroy states.

Aggregation states can be serialized and deserialized to pass over the network during distributed query execution or to write them on disk where there is not enough RAM. They can even be stored in a table with the `DataTypeAggregateFunction` to allow incremental aggregation of data.

> The serialized data format for aggregate function states is not versioned right now. This is ok if aggregate states are only stored temporarily. But we have the `AggregatingMergeTree` table engine for incremental aggregation, and people are already using it in production. This is why we should add support for backward compatibility when changing the serialized format for any aggregate function in the future.

## Server

The server implements several different interfaces:

- An HTTP interface for any foreign clients.
- A TCP interface for the native ClickHouse client and for cross-server communication during distributed query execution.
- An interface for transferring data for replication.

Internally, it is just a basic multithreaded server without coroutines, fibers, etc. Since the server is not designed to process a high rate of simple queries but is intended to process a relatively low rate of complex queries, each of them can process a vast amount of data for analytics.

The server initializes the `Context` class with the necessary environment for query execution: the list of available databases, users and access rights, settings, clusters, the process list, the query log, and so on. This environment is used by interpreters.

We maintain full backward and forward compatibility for the server TCP protocol: old clients can talk to new servers and new clients can talk to old servers. But we don't want to maintain it eternally, and we are removing support for old versions after about one year.

> For all external applications, we recommend using the HTTP interface because it is simple and easy to use. The TCP protocol is more tightly linked to internal data structures: it uses an internal format for passing blocks of data and it uses custom framing for compressed data. We haven't released a C library for that protocol because it requires linking most of the ClickHouse codebase, which is not practical.

## Distributed Query Execution

Servers in a cluster setup are mostly independent. You can create a `Distributed` table on one or all servers in a cluster. The `Distributed` table does not store data itself – it only provides a "view" to all local tables on multiple nodes of a cluster. When you SELECT from a `Distributed` table, it rewrites that query, chooses remote nodes according to load balancing settings, and sends the query to them. The `Distributed` table requests remote servers to process a query just up to a stage where intermediate results from different servers can be merged. Then it receives the intermediate results and merges them. The distributed table tries to distribute as much work as possible to remote servers, and does not send much intermediate data over the network.

> Things become more complicated when you have subqueries in IN or JOIN clauses and each of them uses a `Distributed` table. We have different strategies for execution of these queries.

There is no global query plan for distributed query execution. Each node has its own local query plan for its part of the job. We only have simple one-pass distributed query execution: we send queries for remote nodes and then merge the results. But this is not feasible for difficult queries with high cardinality GROUP BYs or with a large amount of temporary data for JOIN: in such cases, we need to "reshuffle" data between servers, which requires additional coordination. ClickHouse does not support that kind of query execution, and we need to work on it.

## Merge Tree

`MergeTree` is a family of storage engines that supports indexing by primary key. The primary key can be an arbitrary tuple of columns or expressions. Data in a `MergeTree` table is stored in "parts". Each part stores data in the primary key order (data is ordered lexicographically by the primary key tuple). All the table columns are stored in separate `column.bin` files in these parts. The files consist of compressed blocks. Each block is usually from 64 KB to 1 MB of uncompressed data, depending on the average value size. The blocks consist of column values placed contiguously one after the other. Column values are in the same order for each column (the order is defined by the primary key), so when you iterate by many columns, you get values for the corresponding rows.

The primary key itself is "sparse". It doesn't address each single row, but only some ranges of data. A separate `primary.idx` file has the value of the primary key for each N-th row, where N is called `index_granularity` (usually, N = 8192). Also, for each column, we have `column.mrk` files with "marks," which are offsets to each N-th row in the data file. Each mark is a pair: the offset in the file to the beginning of the compressed block, and the offset in the decompressed block to the beginning of data. Usually compressed blocks are aligned by marks, and the offset in the decompressed block is zero. Data for `primary.idx` always resides in memory and data for `column.mrk` files is cached.

When we are going to read something from a part in `MergeTree`, we look at `primary.idx` data and locate ranges that could possibly contain requested data, then look at `column.mrk` data and calculate offsets for where to start reading those ranges. Because of sparseness, excess data may be read. ClickHouse is not suitable for a high load of simple point queries, because the entire range with `index_granularity` rows must be read for each key, and the entire compressed block must be decompressed for each column. We made the index sparse because we must be able to maintain trillions of rows per single server without noticeable memory consumption for the index. Also, because the primary key is sparse, it is not unique: it cannot check the existence of the key in the table at INSERT time. You could have many rows with the same key in a table.

When you `INSERT` a bunch of data into `MergeTree`, that bunch is sorted by primary key order and forms a new part. To keep the number of parts relatively low, there are background threads that periodically select some parts and merge them to a single sorted part. That's why it is called `MergeTree`. Of course, merging leads to "write amplification". All parts are immutable: they are only created and deleted, but not modified. When SELECT is run, it holds a snapshot of the table (a set of parts). After merging, we also keep old parts for some time to make recovery after failure easier, so if we see that some merged part is probably broken, we can replace it with its source parts.

`MergeTree` is not an LSM tree because it doesn't contain "memtable" and "log": inserted data is written directly to the filesystem. This makes it suitable only to INSERT data in batches, not by individual row and not very frequently – about once per second is ok, but a thousand times a second is not. We did it this way for simplicity's sake, and because we are already inserting data in batches in our applications.

> MergeTree tables can only have one (primary) index: there aren't any secondary indices. It would be nice to allow multiple physical representations under one logical table, for example, to store data in more than one physical order or even to allow representations with pre-aggregated data along with original data.

There are MergeTree engines that are doing additional work during background merges. Examples are `CollapsingMergeTree` and `AggregatingMergeTree`. This could be treated as special support for updates. Keep in mind that these are not real updates because users usually have no control over the time when background merges will be executed, and data in a `MergeTree` table is almost always stored in more than one part, not in completely merged form.

## Replication

Replication in ClickHouse is implemented on a per-table basis. You could have some replicated and some non-replicated tables on the same server. You could also have tables replicated in different ways, such as one table with two-factor replication and another with three-factor.

Replication is implemented in the `ReplicatedMergeTree` storage engine. The path in `ZooKeeper` is specified as a parameter for the storage engine. All tables with the same path in `ZooKeeper` become replicas of each other: they synchronize their data and maintain consistency. Replicas can be added and removed dynamically simply by creating or dropping a table.

Replication uses an asynchronous multi-master scheme. You can insert data into any replica that has a session with `ZooKeeper`, and data is replicated to all other replicas asynchronously. Because ClickHouse doesn't support UPDATEs, replication is conflict-free. As there is no quorum acknowledgment of inserts, just-inserted data might be lost if one node fails.

Metadata for replication is stored in ZooKeeper. There is a replication log that lists what actions to do. Actions are: get part; merge parts; drop partition, etc. Each replica copies the replication log to its queue and then executes the actions from the queue. For example, on insertion, the "get part" action is created in the log, and every replica downloads that part. Merges are coordinated between replicas to get byte-identical results. All parts are merged in the same way on all replicas. To achieve this, one replica is elected as the leader, and that replica initiates merges and writes "merge parts" actions to the log.

Replication is physical: only compressed parts are transferred between nodes, not queries. To lower the network cost (to avoid network amplification), merges are processed on each replica independently in most cases. Large merged parts are sent over the network only in cases of significant replication lag.

In addition, each replica stores its state in ZooKeeper as the set of parts and its checksums. When the state on the local filesystem diverges from the reference state in ZooKeeper, the replica restores its consistency by downloading missing and broken parts from other replicas. When there is some unexpected or broken data in the local filesystem, ClickHouse does not remove it, but moves it to a separate directory and forgets it.

> The ClickHouse cluster consists of independent shards, and each shard consists of replicas. The cluster is not elastic, so after adding a new shard, data is not rebalanced between shards automatically. Instead, the cluster load will be uneven. This implementation gives you more control, and it is fine for relatively small clusters such as tens of nodes. But for clusters with hundreds of nodes that we are using in production, this approach becomes a significant drawback. We should implement a table engine that will span its data across the cluster with dynamically replicated regions that could be split and balanced between clusters automatically.


[Original article](https://clickhouse.tech/docs/en/development/architecture/) <!--hide-->
