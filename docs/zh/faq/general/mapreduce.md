---
title: 为何不使用 MapReduce等技术?
toc_hidden: true
sidebar_position: 110
---

# 为何不使用 MapReduce等技术? {#why-not-use-something-like-mapreduce}

我们可以将MapReduce这样的系统称为分布式计算系统，其中的reduce操作是基于分布式排序的。这个领域中最常见的开源解决方案是[Apache Hadoop](http://hadoop.apache.org)。

这些系统不适合用于在线查询，因为它们的延迟很大。换句话说，它们不能被用作网页界面的后端。这些类型的系统对于实时数据更新并不是很有用。如果操作的结果和所有中间结果(如果有的话)都位于单个服务器的内存中，那么分布式排序就不是执行reduce操作的最佳方式，这通常是在线查询的情况。在这种情况下，哈希表是执行reduce操作的最佳方式。优化map-reduce任务的一种常见方法是使用内存中的哈希表进行预聚合(部分reduce)。用户手动执行此优化。在运行简单的map-reduce任务时，分布式排序是导致性能下降的主要原因之一。

大多数MapReduce实现允许你在集群中执行任意代码。但是声明性查询语言更适合于OLAP，以便快速运行实验。例如，Hadoop有Hive和Pig。还可以考虑使用Cloudera Impala或Shark(已经过时了)来支持Spark，以及Spark SQL、Presto和Apache Drill。与专门的系统相比，运行这些任务的性能是非常不理想的，但是相对较高的延迟使得使用这些系统作为web界面的后端是不现实的。
