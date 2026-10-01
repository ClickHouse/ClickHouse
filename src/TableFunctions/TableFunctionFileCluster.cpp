#include <Core/Settings.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFileCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <TableFunctions/registerTableFunctions.h>

#include <memory>

namespace DB
{
namespace Setting
{
    extern const SettingsString rename_files_after_processing;
}

StoragePtr TableFunctionFileCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/, bool /*is_insert_query*/) const
{
    StoragePtr storage;

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// On worker node this filename won't contain any globs
        StorageFile::CommonArguments args{
            WithContext(context),
            StorageID(getDatabaseName(), table_name),
            format,
            std::nullopt /*format settings*/,
            compression_method,
            columns,
            ConstraintsDescription{},
            String{},
            context->getSettingsRef()[Setting::rename_files_after_processing]};

        storage = std::make_shared<StorageFile>(StorageFile::FileSource::parse(filename, context), /* distributed_processing = */ true, args);
    }
    else
    {
        storage = std::make_shared<StorageFileCluster>(
            context,
            cluster_name,
            filename,
            format,
            compression_method,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{});
    }

    return storage;
}


void registerTableFunctionFileCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFileCluster>(
        {.description = R"DOCS_MD(
Enables simultaneous processing of files matching a specified path across multiple nodes within a cluster. The initiator establishes connections to worker nodes, expands globs in the file path, and delegates file-reading tasks to worker nodes. Each worker node is querying the initiator for the next file to process, repeating until all tasks are completed (all files are read).

<Note>
This function will operate _correctly_ only in case the set of files matching the initially specified path is identical across all nodes, and their content is consistent among different nodes.  
In case these files differ between nodes, the return value cannot be predetermined and depends on the order in which worker nodes request tasks from the initiator.
</Note>

## Syntax {#syntax}

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

## Arguments {#arguments}

| Argument             | Description                                                                                                                                                                        |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cluster_name`       | Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.                                                                  |
| `path`               | The relative path to the file from [user_files_path](/reference/settings/server-settings/settings#user_files_path). Path to file also supports [globs](#globs-in-path). |
| `format`             | [Format](/reference/formats/index) of the files. Type: [String](/reference/data-types/string).                                                                           |
| `structure`          | Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](/reference/data-types/string).                             |
| `compression_method` | Compression method. Supported compression types are `gz`, `br`, `xz`, `zst`, `lz4`, and `bz2`.                                                                                     |

## Returned value {#returned_value}

A table with the specified format and structure and with data from files matching the specified path.

**Example**

Given a cluster named `my_cluster` and given the following value of setting `user_files_path`:

```bash
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

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

## Globs in Path {#globs-in-path}

All patterns supported by [File](/reference/functions/table-functions/file#globs-in-path) table function are supported by FileCluster.

## Related {#related}

- [File table function](/reference/functions/table-functions/file)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction},
        {.allow_readonly = false});
}

}
