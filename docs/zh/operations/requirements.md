---
toc_priority: 44
toc_title: "要求"
---

# 要求 {#requirements}

## CPU {#cpu}

对于从预构建的deb包进行安装，请使用具有x86\_64架构并支持SSE4.2指令的CPU。 要使用不支持SSE4.2或具有AArch64或PowerPC64LE体系结构的处理器运行ClickHouse，您应该从源代码构建ClickHouse。

ClickHouse实现并行数据处理并使用所有可用的硬件资源。 在选择处理器时，考虑到ClickHouse在具有大量内核但时钟速率较低的配置中的工作效率要高于具有较少内核和较高时钟速率的配置。 例如，具有2600MHz的16核心优于具有3600MHz的8核心。

建议使用 **睿频加速** 和 **超线程** 技术。 它显着提高了典型工作负载的性能。

## RAM {#ram}

我们建议使用至少4GB的RAM来执行重要的查询。 ClickHouse服务器可以使用少得多的RAM运行，但它需要处理查询的内存。

RAM所需的体积取决于:

-   查询的复杂性。
-   查询中处理的数据量。

要计算所需的RAM体积，您应该估计临时数据的大小 [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) 和您使用的其他操作。

ClickHouse可以使用外部存储器来存储临时数据。看 [在外部存储器中分组](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) 有关详细信息。

## 交换文件 {#swap-file}

禁用生产环境的交换文件。

## 存储子系统 {#storage-subsystem}

您需要有2GB的可用磁盘空间来安装ClickHouse。

数据所需的存储量应单独计算。 评估应包括:

-   估计数据量。

    您可以采取数据的样本并从中获取行的平均大小。 然后将该值乘以计划存储的行数。

-   数据压缩系数。

    要估计数据压缩系数，请将数据的样本加载到ClickHouse中，并将数据的实际大小与存储的表的大小进行比较。 例如，点击流数据通常被压缩6-10倍。

要计算要存储的最终数据量，请将压缩系数应用于估计的数据量。 如果计划将数据存储在多个副本中，则将估计的量乘以副本数。

## 网络 {#network}

如果可能的话，使用10G或更高级别的网络。

网络带宽对于处理具有大量中间结果数据的分布式查询至关重要。 此外，网络速度会影响复制过程。

## 软件 {#software}

ClickHouse主要是为Linux系列操作系统开发的。 推荐的Linux发行版是Ubuntu。 `tzdata` 软件包应安装在系统中。

ClickHouse也可以在其他操作系统系列中工作。 查看详细信息 [开始](../getting-started/index.md) 文档的部分。
