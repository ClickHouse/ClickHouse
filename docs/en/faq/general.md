---
toc_priority: 78
toc_title: General Questions
---

# General Questions {#general-questions}

## Why Not Use Something Like MapReduce? {#why-not-use-something-like-mapreduce}

We can refer to systems like MapReduce as distributed computing systems in which the reduce operation is based on distributed sorting. The most common open-source solution in this class is [Apache Hadoop](http://hadoop.apache.org). Yandex uses its in-house solution, YT.

These systems aren’t appropriate for online queries due to their high latency. In other words, they can’t be used as the back-end for a web interface. These types of systems aren’t useful for real-time data updates. Distributed sorting isn’t the best way to perform reduce operations if the result of the operation and all the intermediate results (if there are any) are located in the RAM of a single server, which is usually the case for online queries. In such a case, a hash table is an optimal way to perform reduce operations. A common approach to optimizing map-reduce tasks is pre-aggregation (partial reduce) using a hash table in RAM. The user performs this optimization manually. Distributed sorting is one of the main causes of reduced performance when running simple map-reduce tasks.

Most MapReduce implementations allow you to execute arbitrary code on a cluster. But a declarative query language is better suited to OLAP to run experiments quickly. For example, Hadoop has Hive and Pig. Also consider Cloudera Impala or Shark (outdated) for Spark, as well as Spark SQL, Presto, and Apache Drill. Performance when running such tasks is highly sub-optimal compared to specialized systems, but relatively high latency makes it unrealistic to use these systems as the backend for a web interface.

## What If I Have a Problem with Encodings When Using Oracle Through ODBC? {#oracle-odbc-encodings}

If you use Oracle through the ODBC driver as a source of external dictionaries, you need to set the correct value for the `NLS_LANG` environment variable in `/etc/default/clickhouse`. For more information, see the [Oracle NLS\_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Example**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## How Do I Export Data from ClickHouse to a File? {#how-to-export-to-file}

### Using INTO OUTFILE Clause {#using-into-outfile-clause}

Add an [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) clause to your query.

For example:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

By default, ClickHouse uses the [TabSeparated](../interfaces/formats.md#tabseparated) format for output data. To select the [data format](../interfaces/formats.md), use the [FORMAT clause](../sql-reference/statements/select/format.md#format-clause).

For example:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Using a File-Engine Table {#using-a-file-engine-table}

See [File](../engines/table-engines/special/file.md).

### Using Command-Line Redirection {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

See [clickhouse-client](../interfaces/cli.md).

{## [Original article](https://clickhouse.tech/docs/en/faq/general/) ##}
