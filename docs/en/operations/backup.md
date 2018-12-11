# Data Backup 

While [replication](table_engines/replication.md#table_engines-replication) provides protection from hardware failures, it does not protect against human errors: accidentally deleting data, deleting the wrong table or on the wrong cluster, software bugs leading to incorrect data processing or data corruption. In many cases commands like these will affect all replicas. ClickHouse has built-in safeguards to prevent some of mistakes, for example by default [you can't just drop tables with MergeTree-like engine containing more than 50Gb of data](https://github.com/yandex/ClickHouse/blob/v18.14.18-stable/dbms/programs/server/config.xml#L322-L330), but they don't cover all possible cases and can be circumvented.

So in order to effectively mitigate possible human errors, you should carefully prepare your backup and restore strategy **in advance**. 

Each company has different resources available and different business requirements so there's no universal solution for ClickHouse backups and restores that will fit any situation. What works for gigabyte of data likely won't work for tens of petabytes. There are different possible approaches with their own pros and cons, which will be discussed below. Often it makes to employ few of those instead of just one to compensate their trade-offs.

!!! note "Note"
    Keep in mind that if you backed something up and never tried to restore it, chances are that restore will not work properly when you'll need it (or at least will take longer than business can tolerate). So whatever backup approach you'll choose, make sure to automate restore process too and practice it on spare ClickHouse cluster regularly. 

## Duplicating Source Data Somewhere Else

Often data that is ingested into ClickHouse is delivered through some sort of persistent queue, for example [Apache Kafka](https://kafka.apache.org). In this case it is possible to set up additional set of subscribers that will read same data stream as is written to ClickHouse and store it in some cold storage. Most companies already have some default recommended cold storage, it could be an object store or a distributed filesystem, for example [HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

## Filesystem Snapshots

Some local filesystems provide functionality to create snapshots, for example [ZFS](https://en.wikipedia.org/wiki/ZFS), but they might be not best choice for serving live queries. Possible solution is to create additional replicas with this kind of filesystems and exclude them from distributed tables that are used for SELECT queries. Snapshots on such replicas will be out of reach of any queries modifying data. As a bonus these replicas might have different hardware configuration with much more disks attached per server, which will be cost-effective.

## clickhouse-copier

[clickhouse-copier](utils/clickhouse-copier.md) is a versatile tool initially created to re-shard petabytes-sized tables, but it can be used for backup and restore purposes as well because it just reliably copies data between ClickHouse tables and clusters.

For smaller volumes of data simple `INSERT INTO ... SELECT ...` to remote tables might work as well.

## Manipulations with Parts

ClickHouse allows to create a local copy of table partitions using `ALTER TABLE ... FREZE PARTITION ...` query. It's implemented using hardlinks to `/var/lib/clickhouse/shadow/` folder, so for old data it usually does not consume extra disk space. As created file copies are no longer touched by ClickHouse server, you can just leave them there: it's still be a simple backup that doesn't require any additional external system, but will be prone to hardware issues since. It's better to remotely copy them somewhere and then probably remove the local copies. Distributed filesystems and object stores are still a good options for this, but normal file servers attached with large enough capacity might work as well (in this case the transfer will happen via network filesystem or maybe [rsync](https://en.wikipedia.org/wiki/Rsync)).

More details on queries related to partition manipulations can be found in [respective section of ALTER documentation](../query_language/alter.md#query_language-manipulation-with-partitions-and-parts).

There's a third-party tool to automate this approach: [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup).

## Data Dump

[Console client](../interfaces/cli.md) can be used to run queries like `SELECT * FROM my_table;` to dump the tables into files using any of the [supported serialization formats](../interfaces/formats.md#formats). Though if you are using ClickHouse as intended and have large enough volumes of data, this will hardly be practical.

[Original article](https://clickhouse.yandex/docs/en/operations/access_rights/) <!--hide-->