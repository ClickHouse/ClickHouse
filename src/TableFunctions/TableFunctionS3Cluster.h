#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/**
 * s3Cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
 * A table function, which allows to process many files from S3 on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterics
 * in S3 file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionS3Cluster : public ITableFunction
{
public:
    static constexpr auto name = "s3Cluster";
    std::string getName() const override
    {
        return name;
    }
    bool hasStaticStructure() const override { return true; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "S3Cluster"; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr &, ContextPtr) override;

    String cluster_name;
    String filename;
    String format;
    String structure;
    String access_key_id;
    String secret_access_key;
    String compression_method = "auto";
};

namespace S3ClusterDoc
{
const char * doc = R"(
Allows processing files from [Amazon S3](https://aws.amazon.com/s3/) in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterics in S3 file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

``` sql
s3Cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
```

**Arguments**

-   `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
-   `source` — URL to a file or a bunch of files. Supports following wildcards in readonly mode: `*`, `?`, `{'abc','def'}` and `{N..M}` where `N`, `M` — numbers, `abc`, `def` — strings. For more information see [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path).
-   `access_key_id` and `secret_access_key` — Keys that specify credentials to use with given endpoint. Optional.
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Examples**

Select the data from all files in the cluster `cluster_simple`:

``` sql
SELECT * FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') ORDER BY (name, value, polygon);
```

Count the total amount of rows in all files in the cluster `cluster_simple`:

``` sql
SELECT count(*) FROM s3Cluster('cluster_simple', 'http://minio1:9001/root/data/{clickhouse,database}/*', 'minio', 'minio123', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))');
```

!!! warning "Warning"
    If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.

**See Also**

-   [S3 engine](../../engines/table-engines/integrations/s3.md)
-   [s3 table function](../../sql-reference/table-functions/s3.md)

)";

}

}

#endif
