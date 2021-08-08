---
toc_priority: 68
toc_title: External Disks for Storing Data
---

# External Disks for Storing Data {#external-disks}

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely — on [Amazon s3](https://aws.amazon.com/s3/) disks or in the Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

To work with data stored on `Amazon s3` disks use [s3](../engines/table-engines/integrations/s3.md) table engine, and to work with data in the Hadoop Distributed File System — [HDFS](../engines/table-engines/integrations/hdfs.md) table engine. 

## Zero-copy Replication {#zero-copy}

ClickHouse supports zero-copy replication for `s3` and `HDFS` disks, which means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself. 