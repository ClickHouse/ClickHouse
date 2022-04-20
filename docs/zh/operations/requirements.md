---
sidebar_position: 44
sidebar_label: "必备条件"
---

# 必备条件 {#requirements}

## CPU {#cpu}

如果您使用预编译的DEB/RPM包安装ClickHouse，请使用支持SSE4.2指令集的x86_64架构的CPU。如果需要在不支持SSE4.2指令集的CPU上，或者在AArch64（ARM）和PowerPC64LE（IBM Power）架构上运行ClickHouse，您应该从源码编译ClickHouse。

ClickHouse实现了并行数据处理，处理时会使用所有的可用资源。在选择处理器时，请注意：ClickHouse在具有大量计算核、时钟频率稍低的平台上比计算核少、时钟频率高的平台上效率更高。例如，ClickHouse在16核 2.6GHz的CPU上运行速度高于8核 3.6GHz的CPU。

建议使用 **睿频加速** 和 **超线程** 技术。 它显着提高了正常工作负载的性能。

## RAM {#ram}

我们建议使用至少4GB的内存来执行重要的查询。 ClickHouse服务器可以使用很少的内存运行，但它需要一定量的内存用于处理查询。

ClickHouse所需内存取决于:

- 查询的复杂程度。
- 查询处理的数据量。

要计算所需的内存大小，您应该考虑用于[GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause)、[DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct)、[JOIN](../sql-reference/statements/select/join.md#select-join) 和其他操作所需的临时数据量。

ClickHouse可以使用外部存储器来存储临时数据。详情请见[在外部存储器中分组](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory)。

## 交换文件 {#swap-file}

请在生产环境禁用交换文件。

## 存储子系统 {#storage-subsystem}

您需要有2GB的可用磁盘空间来安装ClickHouse。

数据所需的存储空间应单独计算。预估存储容量时请考虑:

- 数据量

    您可以对数据进行采样并计算每行的平均占用空间。然后将该值乘以计划存储的行数。

- 数据压缩比

    要计算数据压缩比，请将样本数据写入ClickHouse，并将原始数据大小与ClickHouse实际存储的数据进行比较。例如，用户点击行为的原始数据压缩比通常为6-10。

请将原始数据的大小除以压缩比来获得实际所需存储的大小。如果您打算将数据存放于几个副本中，请将存储容量乘上副本数。

## 网络 {#network}

如果可能的话，请使用10G或更高级别的网络。

网络带宽对于处理具有大量中间结果数据的分布式查询至关重要。此外，网络速度会影响复制过程。

## 软件 {#software}

ClickHouse主要是为Linux系列操作系统开发的。推荐的Linux发行版是Ubuntu。您需要检查`tzdata`（对于Ubuntu）软件包是否在安装ClickHouse之前已经安装。

ClickHouse也可以在其他操作系统系列中工作。详情请查看[开始](../getting-started/index.md)。
