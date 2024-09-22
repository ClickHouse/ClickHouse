---
slug: /en/sql-reference/table-functions/fileCluster
sidebar_position: 61
sidebar_label: fileCluster
---

# fileCluster Table Function

Enables simultaneous processing of files matching a specified path across multiple nodes within a cluster. The initiator establishes connections to worker nodes, expands globs in the file path, and delegates file-reading tasks to worker nodes. Each worker node is querying the initiator for the next file to process, repeating until all tasks are completed (all files are read).

:::note    
This function will operate _correctly_ only in case the set of files matching the initially specified path is identical across all nodes, and their content is consistent among different nodes.  
In case these files differ between nodes, the return value cannot be predetermined and depends on the order in which worker nodes request tasks from the initiator.
:::

**Syntax**

``` sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

**Arguments**

- `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- `path` — The relative path to the file from [user_files_path](/docs/en/operations/server-configuration-parameters/settings.md#user_files_path). Path to file also supports [globs](#globs-in-path).
- `format` — [Format](../../interfaces/formats.md#formats) of the files. Type: [String](../../sql-reference/data-types/string.md).
- `structure` — Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](../../sql-reference/data-types/string.md).
- `compression_method` — Compression method. Supported compression types are `gz`, `br`, `xz`, `zst`, `lz4`, and `bz2`.

**Returned value**

A table with the specified format and structure and with data from files matching the specified path.

**Example**

Given a cluster named `my_cluster` and given the following value of setting `user_files_path`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
Also, given there are files `test1.csv` and `test2.csv` inside `user_files_path` of each cluster node, and their content is identical across different nodes:
```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

For example, one can create these files by executing these two queries on every cluster node:
```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

Now, read data contents of `test1.csv` and `test2.csv` via `fileCluster` table function:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```


## Globs in Path

All patterns supported by [File](../../sql-reference/table-functions/file.md#globs-in-path) table function are supported by FileCluster.

**See Also**

- [File table function](../../sql-reference/table-functions/file.md)
