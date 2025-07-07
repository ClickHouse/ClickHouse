---
description: 'Details alternative backup or restore methods'
sidebar_label: 'Alternative methods'
slug: /operations/backup/alternative_methods
title: 'Alternative backup or restore methods'
---

# Alternative backup methods

ClickHouse stores data on disk, and there are many ways to back up disks. 
These are some alternatives that have been used in the past, and that may fit
your use case.

### Duplicating source data somewhere else {#duplicating-source-data-somewhere-else}

Often data ingested into ClickHouse is delivered through some sort of persistent
queue, such as [Apache Kafka](https://kafka.apache.org). In this case, it is possible to configure an
additional set of subscribers that will read the same data stream while it is 
being written to ClickHouse and store it in cold storage somewhere. Most companies
already have some default recommended cold storage, which could be an object store
or a distributed filesystem like [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

### Filesystem Snapshots {#filesystem-snapshots}

Some local filesystems provide snapshot functionality (for example, [ZFS](https://en.wikipedia.org/wiki/ZFS)), 
but they might not be the best choice for serving live queries. A possible solution
is to create additional replicas with this kind of filesystem and exclude them 
from the [Distributed](../engines/table-engines/special/distributed.md) tables that are used for `SELECT` queries. 
Snapshots on such replicas will be out of reach of any queries that modify data.
As a bonus, these replicas might have special hardware configurations with more 
disks attached per server, which would be cost-effective.

For smaller volumes of data, a simple `INSERT INTO ... SELECT ...` to remote tables
might work as well.

### Manipulations with Parts {#manipulations-with-parts}

ClickHouse allows using the `ALTER TABLE ... FREEZE PARTITION ...` query to create
a local copy of table partitions. This is implemented using hardlinks to the `/var/lib/clickhouse/shadow/`
folder, so it usually does not consume extra disk space for old data. The created 
copies of files are not handled by ClickHouse server, so you can just leave them there:
you will have a simple backup that does not require any additional external system,
but it will still be prone to hardware issues. For this reason, it's better to 
remotely copy them to another location and then remove the local copies. 
Distributed filesystems and object stores are still a good options for this, 
but normal attached file servers with a large enough capacity might work as well
(in this case the transfer will occur via the network filesystem or maybe [rsync](https://en.wikipedia.org/wiki/Rsync)).
Data can be restored from backup using the `ALTER TABLE ... ATTACH PARTITION ...`

For more information about queries related to partition manipulations, see the 
[`ALTER` documentation](/sql-reference/statements/alter/partition).

A third-party tool is available to automate this approach: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).
