---
toc_priority: 62
toc_title: 架构概述
---

# ClickHouse 架构概述 {#overview-of-clickhouse-architecture}

ClickHouse是一个真正面向列的DBMS。数据按列存储，并在执行数组（向量或列块）期间存储。 只要有可能，操作都是在数组上调度的，而不是在单个值上调度。它被称为“向量化查询执行”，它有助于降低实际数据处理的成本。

> 这个想法并不新。它可以追溯到`APL`（一种编程语言，1957年）及其后代：`A +`（APL方言）、`J`（1990年）、`K`（1993年）和`Q`（来自Kx系统，2003年）。数组编程用于科学数据处理。这个想法在关系数据库中也不是什么新鲜事物：例如，它被用于“VectorWise”系统（也被Actian Corporation称为 Actian Vector Analytic Database）。

有两种不同的方法可以加速查询处理：向量化查询执行和运行时代码生成。后者删除了所有间接和动态调度。这两种方法都没有严格意义上的优于另一种。运行时代码生成可以更好地融合许多操作，从而充分利用 CPU 执行单元和管道。向量化查询执行可能不太实用，因为它涉及必须写入缓存并读回的临时向量。如果临时数据不适合L2缓存，这就会成为一个问题。但是矢量化查询执行更容易利用CPU的SIMD功能。我们朋友写的一篇[research paper](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf)表明最好将两种方法结合起来。ClickHouse使用矢量化查询执行，并且对运行时代码生成的初始支持有限。

## 列 {#columns}

`IColumn`接口用于表示内存中的列（实际上是列的块）。该接口为各种关系运算符的实现提供了辅助方法。几乎所有操作都是不可变的：它们不会修改原始列，而是创建一个新的修改过的列。例如，`IColumn :: filter`方法接受过滤器字节掩码。 它用于`WHERE`和`HAVING`关系运算符。 其他示例：支持`ORDER BY`的`IColumn :: permute`方法，支持`LIMIT`的`IColumn :: cut`方法。

各种`IColumn`实现（`ColumnUInt8`、`ColumnString`等）负责列的内存布局。 内存布局通常是一个连续的数组。对于整数类型的列，它只是一个连续的数组，就像`std :: vector`。对于`String`和`Array`列，它是两个向量：一个用于所有数组元素，连续放置，第二个用于每个数组开头的偏移量。还有`ColumnConst`只在内存中存储一个值，但看起来像一列。

## 字段属性 {#field}

尽管如此，也可以使用单个值。为了表示单个值，使用了`Field`。`Field`只是`UInt64`、`Int64`、`Float64`、`String` 和`Array`的有区别的并集。`IColumn`有`operator []`方法来获取第n个值作为`Field`，以及`insert`方法将一个`Field`附加到列的末尾。这些方法效率不高，因为它们需要处理表示单个值的临时`Field`对象。还有更高效的方法，比如`insertFrom`、`insertRangeFrom`等。

`Field`没有关于表特定数据类型的足够信息。例如，`UInt8`、`UInt16`、`UInt32` 和`UInt64`在`Field`中都表示为`UInt64`。

## Leaky Abstractions {#leaky-abstractions}

`IColumn`有通用的数据关系转换方法，但不能满足所有需求。例如，`ColumnUInt64`没有计算两列总和的方法，而`ColumnString`没有运行子字符串搜索的方法。这些无数的例程是在`IColumn`之外实现的。

列上的各种功能可以使用`IColumn`方法提取`Field`值以通用的、非有效的方式实现，或者使用特定`IColumn`实现中数据的内部内存布局知识以特殊方式实现。它是通过将函数转换为特定的`IColumn`类型并直接处理内部表示来实现的。例如，`ColumnUInt64`具有`getData`方法，该方法返回对内部数组的引用，然后一个单独的线程直接读取或填充该数组。 我们有"leaky abstractions"来允许对各种线程进行有效的专业化。

## 数据类型 {#data_types}

`IDataType`负责序列化和反序列化：用于以二进制或文本形式读取和写入大块的列或单个值。`IDataType`直接对应表中的数据类型。比如有`DataTypeUInt32`、`DataTypeDateTime`、`DataTypeString`等等。

`IDataType`和`IColumn`只是彼此松散的关联。不同的数据类型可以通过相同的`IColumn`实现在内存中表示。例如，`DataTypeUInt32`和`DataTypeDateTime`都由`ColumnUInt32`或`ColumnConstUInt32`表示。此外，相同的数据类型可以由不同的`IColumn`实现来表示。例如，`DataTypeUInt8`可以用`ColumnUInt8`或`ColumnConstUInt8`表示。

`IDataType`只存储元数据。例如，`DataTypeUInt8`根本不存储任何东西（除了虚拟指针`vptr`），而`DataTypeFixedString`只存储`N`（固定大小字符串的大小）。

`IDataType`有各种数据格式的辅助方法。示例是使用引用序列化值、序列化JSON以及将值序列化为XML格式的一部分的方法。与数据格式没有直接对应关系。例如，不同的数据格式`Pretty`和`TabSeparated`可以使用来自`IDataType`接口的相同`serializeTextEscaped`辅助方法。

## Block {#block}

`Block`是表示内存中表的子集（块）的容器。它只是一组三元组：`(IColumn, IDataType, column name)`。在查询执行期间，数据由`Block`处理。如果我们有一个`Block`，我们就有数据（在`IColumn`对象中），我们有关于它的类型的信息（在`IDataType`中），告诉我们如何处理该列，我们有列名。它可以是表中的原始列名，也可以是为获取临时计算结果而分配的一些人为名称。

当我们在块中的列上计算某个函数时，我们将另一列及其结果添加到块中，并且我们不接触函数参数的列，因为操作是不可变的。稍后，可以从块中删除不需要的列，但不能修改。 便于消除公共子表达式。

为每个处理过的数据创建块。请注意，对于相同类型的计算，不同块的列名和类型保持不变，仅列数据发生变化。最好从块头拆分块数据，因为小块大小具有用于复制shared_ptrs和列名的临时字符串的高开销。

## Block Streams {#block-streams}

Block Streams用于处理数据。我们使用Block Streams从某处读取数据、执行数据转换或将数据写入某处。`IBlockInputStream`具有`read`方法来在可用时获取下一个块。`IBlockOutputStream`有`write`方法可以将块推送到某处。

Streams主要负责:

1.  读取或写入表。该表仅返回用于读取或写入块的流。
2.  实现数据格式。 例如，如果您想以`Pretty`格式将数据输出到终端，您可以创建一个块输出流，在其中推送块，并对其进行格式化。
3.  执行数据转换。假设你有`IBlockInputStream`并且想要创建一个过滤流。您创建`FilterBlockInputStream`并使用您的流对其进行初始化。然后，当您从`FilterBlockInputStream`中拉出一个块时，它会从您的流中拉出一个块，对其进行过滤，并将过滤后的块返回给您。查询执行管道以这种方式表示。

还有更复杂的转换。例如，当您从`AggregatingBlockInputStream`中提取时，它会从其源读取所有数据，对其进行聚合，然后为您返回聚合数据流。另一个例子：`UnionBlockInputStream`在构造函数中接受许多输入源以及许多线程。它启动多个线程并并行读取多个源。

> Block Streams使用“pull”方法来控制流：当您从第一个流中拉出一个块时，它从嵌套流中拉出所需的块，并且整个执行管道都将工作。“pull”和“push”都不是最好的解决方案，因为控制流是隐式的，这限制了各种功能的实现，比如同时执行多个查询（将许多管道合并在一起）。可以通过线程或仅运行相互等待的额外线程来克服此限制。如果我们明确控制流，我们可能会有更多的可控性：如果我们找到将数据从一个计算单元传递到这些计算单元之外的另一个计算单元的逻辑. 阅读[文章](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/).

我们应该注意到查询执行管道在每一步都会创建临时数据。我们尽量保持块大小足够小，以便临时数据适配CPU缓存。在这种假设下，与其他计算相比，临时数据的写入和读取几乎是免费的。我们可以考虑另一种选择，那就是将许多操作融合在一起。它可以使管道尽可能短，并删除许多临时数据，这可能是一个优点，但也有缺点。例如，拆分管道可以轻松实现缓存中间数据、同时运行的类似查询中窃取中间数据以及为类似查询合并管道。

## Formats {#formats}

数据格式通过Block Streams实现。有一些“表示”格式仅适用于向客户端输出数据，例如`Pretty`格式，它只提供`IBlockOutputStream`。还有输入/输出格式，如`TabSeparated`或`JSONEachRow`。

还有行流：`IRowInputStream`和`IRowOutputStream`。它们允许您按单行而不是按块拉/推数据。它们仅用于简化面向行格式的实现。包装器`BlockInputStreamFromRowInputStream`和`BlockOutputStreamFromRowOutputStream`允许您将面向行的流转换为常规的面向块的流。

## I/O {#io}

对于面向字节的输入/输出，有`ReadBuffer`和`WriteBuffer`抽象类。它们被用来代替C++的`iostream`。别担心：每个成熟的C++项目都有充分的理由使用`iostream`以外的东西。

`ReadBuffer`和`WriteBuffer`只是一个连续的缓冲区和一个指向该缓冲区中位置的游标。实现可能拥有或不拥有缓冲区的内存。有一种虚拟方法可以用以下数据填充缓冲区（对于`ReadBuffer`）或在某处刷新缓冲区（对于`WriteBuffer`）。虚拟方法很少被调用。

`ReadBuffer`/`WriteBuffer`的实现用于处理文件和文件描述符和sockets，用于实现压缩（`CompressedWriteBuffer`用另一个`WriteBuffer`初始化并在向其写入数据之前执行压缩），以及用于其他的——`ConcatReadBuffer`、`LimitReadBuffer`和`HashingWriteBuffer`。

Read/WriteBuffers只处理字节。`ReadHelpers`和`WriteHelpers`头文件中的函数可以帮助格式化输入/输出。例如，有一些助手可以用十进制格式写一个数字。

让我们看看当您想将`JSON`格式的结果集写入`stdout`时会发生什么。您已准备好从`IBlockInputStream`中获取结果集。您创建`WriteBufferFromFileDescriptor(STDOUT_FILENO)`以将字节写入标准输出。你创建了`JSONRowOutputStream`，用`WriteBuffer`初始化，将`JSON`中的行写入标准输出。你在它上面创建了`BlockOutputStreamFromRowOutputStream`，将它表示为`IBlockOutputStream`。然后你调用`copyData`将数据从`IBlockInputStream`传输到`IBlockOutputStream`。在内部，`JSONRowOutputStream`将编写各种JSON分隔符，并使用对`IColumn`的引用和行号作为参数调用`IDataType::serializeTextJSON`方法。因此，`IDataType::serializeTextJSON`将从`WriteHelpers.h`调用一个方法：例如，`writeText`用于数字类型，`writeJSONString`用于`DataTypeString`。

## 数据表 {#tables}

`IStorage`接口代表表。该接口的不同实现是不同的表引擎。例如`StorageMergeTree`、`StorageMemory`等。这些类的实例只是表。

`IStorage`的关键方法是`read`, `write`。还有`alter`、`rename`、`drop`等等。`read`方法接受以下参数：要从表中读取的列集、要考虑的`AST`查询以及要返回的所需流数。它返回一个或多个`IBlockInputStream`对象和关于在查询执行期间在表引擎中完成的数据处理阶段的信息。

在大多数情况下，read方法只负责从表中读取指定的列，而不负责任何进一步的数据处理。所有进一步的数据处理都是由查询解释器完成的，并且不在`IStorage`的职责范围内。

但也有明显的例外:

-   AST查询被传递给`read`方法，表引擎可以使用它得出索引使用情况，并从表中读取更少的数据。
-   有时表引擎可以自己处理数据到特定的阶段。例如，`StorageDistributed`可以向远程服务器发送一个查询，要求它们将数据处理到一个可以合并来自不同远程服务器的数据的阶段，并返回预处理的数据。然后查询解释器完成数据的处理。

`read`方法可以返回多个`IBlockInputStream`对象以允许并行数据处理。这些多个块输入流可以并行地从表中读取数据。然后你可以用各种可以独立计算的转换(比如表达式求值或过滤)包装这些流，并在它们上面创建一个`UnionBlockInputStream`，以并行地从多个流读取。

还有`TableFunction`。这些函数返回一个临时的`IStorage`对象，用于查询的`FROM`子句。

要快速了解如何实现表引擎，可以看一些简单的东西，比如`StorageMemory`或`StorageTinyLog`。

> 作为`read`方法的结果，`IStorage`返回`QueryProcessingStage` —— 关于查询的哪些部分已经在存储中计算过的信息。

## 解析 {#parsers}

手写的递归解析器用于解析一个查询。例如，`ParserSelectQuery`只是为查询的各个部分递归调用底层解析器。解析器创建一个`AST`。 `AST`由节点表示，节点是`IAST`的实例。

> 由于历史原因，不使用解析生成器。

## 解释器 {#interpreters}

解析器负责从`AST`创建查询执行管道。简单的解析器，例如`InterpreterExistsQuery`和`InterpreterDropQuery`，或者更复杂的`InterpreterSelectQuery`。查询执行管道是块输入或输出流的组合。例如，解析`SELECT`查询的结果是从`IBlockInputStream`读取结果集； INSERT查询的结果是`IBlockOutputStream`写入数据以进行插入，解析`INSERT SELECT`查询的结果是`IBlockInputStream`，它在第一次读取时返回一个空结果集，但从`SELECT`到`INSERT`同时进行。

`InterpreterSelectQuery`使用`ExpressionAnalyzer`和`ExpressionActions`机制进行查询分析和转换。这是大多数基于规则的查询优化完成的地方。`ExpressionAnalyzer`非常混乱，应该重写：应该将各种查询转换和优化提取到单独的类中，以允许对查询进行模块化转换。

## 函数 {#functions}

有普通函数和聚合函数。有关聚合函数，请参见下一节。

普通函数不会改变行数——它们的工作方式就像独立处理每一行一样。事实上，函数不是针对单个行调用的，而是针对数据的`Block`来实现向量化的查询执行。

还有一些其他的函数，如[blockSize](../sql-reference/functions/other-functions.md#function-blocksize), [rowNumberInBlock](../sql-reference/functions/other-functions.md#function-rownumberinblock), [runningAccumulate](../sql-reference/functions/other-functions.md#runningaccumulate), 利用块处理并违反行独立性。

ClickHouse具有强类型，因此不存在隐式类型转换。如果函数不支持特定类型的组合，它会抛出异常。但是对于许多不同类型的组合，函数可以工作(重载)。例如，`plus`函数(实现`+`操作符)适用于任何数字类型的组合:`UInt8` + `Float32`， `UInt16` + `Int8`，等等。此外，一些可变参数函数可以接受任意数量的参数，例如`concat`函数。

实现函数可能有点不方便，因为函数显式地分派支持的数据类型和支持的`IColumns`。例如，`plus`函数的代码是通过实例化一个c++模板生成的，该模板对应于数字类型的每个组合，以及常量或非常量左右参数。

它是实现运行时代码生成以避免模板代码膨胀的绝佳场所。此外，它还可以添加融合函数(如融合乘法-加法)或在一个循环迭代中进行多次比较。

由于执行向量化查询，函数不会发生短路。例如，如果你写`WHERE f(x) AND g(y)`，当`f(x)`为零时(除了`f(x)`是零常量表达式)，即使是行，两边都要计算。但如果`f(x)`条件的选择性很高，而`f(x)`的计算比`g(y)`要便宜得多，那么最好实现多遍计算。它首先计算`f(x)`，然后根据结果过滤列，然后只对较小的、过滤过的数据块计算`g(y)`。

## 聚合函数 {#aggregate-functions}

聚合函数是有状态函数。它们将传递的值积累到某个状态，并允许您从该状态获得结果。它们是通过`IAggregateFunction`接口管理的。状态可以很简单(`AggregateFunctionCount`的状态只是一个`UInt64`值)，也可以很复杂(`AggregateFunctionUniqCombined`的状态是线性数组、哈希表和`HyperLogLog`概率数据结构的组合)。

状态在`Arena`(内存池)中分配，以在执行高基数的`GROUP BY`查询时处理多个状态。状态可以有一个非常重要的构造函数和析构函数:例如，复杂的聚合状态可以自己分配额外的内存。它需要注意创建和销毁状态并正确传递它们的所有权和销毁顺序。

聚合状态可以被序列化和反序列化以在分布式查询执行期间通过网络传递或将它们写入没有足够RAM的磁盘上。它们甚至可以存储在带有`DataTypeAggregateFunction`的表中，以允许数据的增量聚合。

> 聚合函数状态的序列化数据格式现在没有版本化。如果聚合状态只是临时存储，则可以。但是我们有用于增量聚合的`AggregatingMergeTree`表引擎，并且人们已经在生产中使用它。这就是将来在更改任何聚合函数的序列化格式时需要向后兼容的原因。

## 服务 {#server}

服务器实现了几个不同的接口：

-   任何外部客户端HTTP接口。
-   本地ClickHouse客户端和分布式查询执行期间跨服务器通信的TCP接口。
-   用于传输数据以进行复制的接口。

在内部，它只是一个没有协程或纤程的原始多线程服务器。由于服务器的设计目的不是处理高速率的简单查询，而是处理相对较低速率的复杂查询，因此每一个服务器都可以处理大量数据进行分析。

服务器使用查询执行所需的环境初始化`Context`类:可用数据库列表、用户和访问权限、设置、集群、进程列表、查询日志，等等。解释器使用这种环境。

我们维护服务器TCP协议的完全向后和向前兼容性:旧客户端可以与新服务器通信，新客户端可以与旧服务器通信。但是我们不想永远维护它，并且在大约一年后我们会移除对旧版本的支持。

!!! note "注意"
对于大多数外部应用程序，我们建议使用HTTP接口，因为它简单易用。TCP协议与内部数据结构的链接更加紧密:它使用内部格式传递数据块，并使用自定义的帧来压缩数据。我们还没有为该协议发布一个C库，因为它需要链接大多数ClickHouse代码库，这是不实际的。

## 分布式查询 {#distributed-query-execution}

集群设置中的服务器大多是独立的。您可以在集群中的一个或所有服务器上创建一个`Distributed`表。`Distributed`表本身并不存储数据——它只提供一个视图，以查看集群中多个节点上的所有本地表。当您从`Distributed`表中进行SELECT时，它会重写查询，根据负载均衡设置选择远程节点，并将查询发送给它们。`Distributed`表请求远程服务器处理一个查询，直到可以合并来自不同服务器的中间结果的阶段。然后它接收中间结果并合并它们。分布式表试图将尽可能多的工作分配到远程服务器，而不通过网络发送太多中间数据。

当在in或JOIN子句中有子查询，并且每个子查询都使用一个`Distributed`表时，情况会变得更加复杂。我们有不同的策略来执行这些查询。

分布式查询执行没有全局查询计划。每个节点都有其作业部分的本地查询计划。我们只有简单的一次执行分布式查询:我们向远程节点发送查询，然后合并结果。但是对于具有高基数GROUP by的复杂查询或具有大量用于JOIN的临时数据，这是不可行的。在这种情况下，我们需要在服务器之间重新清洗数据，这需要额外的协调。ClickHouse不支持这种查询执行，我们需要对它进行处理。

## Merge Tree {#merge-tree}

`MergeTree`是一组支持主键索引的存储引擎。主键可以是任意的列或表达式元组。`MergeTree`表中的数据存储在“parts”中。每个部分以主键顺序存储数据，因此数据按照字典顺序按主键元组排序。所有表列都存储在这些部分的`column.bin`文件中。文件由压缩块组成。每个块通常是64 KB到1 MB的未压缩数据，具体取决于平均值大小。块由一个接一个连续放置的列值组成。每个列的列值顺序相同(主键定义顺序)，因此当您迭代多个列时，您将获得相应行的值。

主键本身是“稀疏的”。它不处理每一行，而只处理某些范围的数据。一个单独的初选。`primary.idx`文件具有每个第N行的主键值，其中N被称为`index_granularity`(通常N = 8192)。同样，对于每一列，我们有`column.mrk`文件的“标记”，这是数据文件中每个第n行的偏移量。每个标记是一对:文件到压缩块开始的偏移量，以及解压缩块到数据开始的偏移量。通常，压缩块按标记对齐，解压缩块中的偏移量为零。`primary.idx`始终驻留在内存中，`column.mrk`列的数据文件被缓存。

当我们要从`MergeTree`中的一个部分读取一些东西时，我们会查看`primary.idx`数据并定位可能包含请求数据的范围，然后查看`column.mrk`数据并计算从哪里开始读取的偏移量那些范围。由于稀疏，可能会读取多余的数据。 ClickHouse不适合单点查询的高负载，因为必须为每个键读取带有`index_granularity`行的整个范围，并且必须为每一列解压整个压缩块。 我们使索引变得稀疏，因为我们必须能够在每台服务器上维护数万亿行而不会对索引造成明显的内存消耗。此外，由于主键是稀疏的，它不是唯一的：它无法在INSERT时检查表中键的存在。一个表中可以有许多具有相同键的行。

当您将一组数据`INSERT`到`MergeTree`中时，该组数据按主键顺序排序并形成一个新部分。 有后台线程会定期选择一些部分并将它们合并为一个已排序的部分，以保持部分数量相对较少。 这就是它被称为`MergeTree`的原因。当然，合并会导致“写放大”。所有部分都是不可变的：它们只会被创建和删除，而不会被修改。 执行SELECT时，它保存表的快照（一组parts）。合并后，我们也会将旧部件保留一段时间，以便故障后更容易恢复，因此如果我们发现某些合并部件可能损坏了，我们可以将其替换为源parts。

`MergeTree`不是LSM树，因为它不包含“memtable”和“log”：插入的数据直接写入文件系统。这使得它仅适用于批量插入数据，而不是逐行插入，也不是很频繁——大约每秒一次就可以了，但每秒一千次就不行了。我们这样做是为了简单起见，因为我们已经在我们的应用程序中批量插入数据。

有MergeTree引擎在后台合并期间做额外的工作。例如`CollapsingMergeTree`和`AggregatingMergeTree`。这可以被视为对更新的特殊支持。 请记住，这些并不是真正的更新，因为用户通常无法控制执行后台合并的时间，并且`MergeTree`表中的数据几乎总是存储在多个part，而不是完全合并的形式。

## 复制 {#replication}

ClickHouse中的复制可以在每个表的基础上配置。在同一台服务器上可以有一些复制的表和一些非复制的表。还可以以不同的方式复制表，例如一个表具有两因素复制，另一个表具有三因素复制。

复制是在`ReplicatedMergeTree`存储引擎中实现的。其中`ZooKeeper`中的路径作为存储引擎的参数。所有在`ZooKeeper`中具有相同路径的表成为彼此的副本:它们同步它们的数据并保持一致性。只需创建或删除一个表，就可以动态地添加和删除副本。

复制使用异步多主机方案。您可以将数据插入到任何与`ZooKeeper`有会话的副本中，并将数据异步复制到所有其他副本中。因为ClickHouse不支持更新，所以复制是无冲突的。由于没有插入的仲裁确认，如果一个节点故障，刚刚插入的数据可能会丢失。

用于复制的元数据存储在ZooKeeper中。这里有一个复制日志，列出了要执行的操作。行动是:得到部分;merge parts;删除一个分区，等等。每个副本将复制日志复制到其队列，然后从队列执行操作。例如，在插入时，在日志中创建“获取part”操作，每个副本都下载该part。在副本之间协调合并以获得字节相同的结果。所有部分在所有副本上以相同的方式合并。其中一个领导者首先发起一个新的合并，并将“merge parts”操作写入日志。多个副本(或所有副本)可以同时是leader。可以使用`merge_tree`设置`replicated_can_become_leader`来阻止副本成为leader。leader负责安排后台合并。

复制是物理的:只有压缩的部分在节点之间传输，而不是查询。在大多数情况下，合并是在每个副本上独立进行的，以避免网络扩大化，降低网络成本。较大的合并部分只在复制严重滞后的情况下通过网络发送。

此外，每个副本在ZooKeeper中以部件集及其校验和的形式存储自己的状态。当本地文件系统的状态与ZooKeeper中的引用状态偏离时，副本通过从其他副本中下载丢失和损坏的部分来恢复一致性。当本地文件系统中有一些意外或损坏的数据时，ClickHouse不会删除它，而是将它移动到一个单独的目录并忘记它。

!!! note "注意"
ClickHouse集群由独立的分片组成，每个分片由副本组成。集群**无弹性**，因此添加新分片后，不会自动在分片之间重新平衡数据。相反，应该将集群负载调整为不均匀。这个实现给了你更多的控制权，对于相对较小的集群来说是可以的，比如几十个节点。但是对于我们在生产中使用的具有数百个节点的集群，这种方法成为一个明显的缺点。我们应该实现一个跨越集群的表引擎，动态复制的区域可以在集群之间自动分割和平衡。

{## [原始文章](https://clickhouse.tech/docs/en/development/architecture/) ##}
