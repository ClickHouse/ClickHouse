#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>

namespace DB
{

class Context;

/* s3(source, [access_key_id, secret_access_key,] format, structure) - creates a temporary storage for a file in S3
 */
class TableFunctionS3 : public ITableFunction
{
public:
    static constexpr auto name = "s3";
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

    const char * getStorageTypeName() const override { return "S3"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String filename;
    String format;
    String structure;
    String access_key_id;
    String secret_access_key;
    String compression_method = "auto";
};

class TableFunctionCOS : public TableFunctionS3
{
public:
    static constexpr auto name = "cosn";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "COSN"; }
};

namespace S3Doc
{
const char * doc = R"(
Provides table-like interface to select/insert files in [Amazon S3](https://aws.amazon.com/s3/). This table function is similar to [hdfs](../../sql-reference/table-functions/hdfs.md), but provides S3-specific features.

**Syntax**

``` sql
s3(path, [aws_access_key_id, aws_secret_access_key,] format, structure, [compression])
```

**Arguments**

-   `path` — Bucket url with path to file. Supports following wildcards in readonly mode: `*`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. For more information see [here](../../engines/table-engines/integrations/s3.md#wildcards-in-path).
-   `format` — The [format](../../interfaces/formats.md#formats) of the file.
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.
-   `compression` — Parameter is optional. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension.

**Returned value**

A table with the specified structure for reading or writing data in the specified file.

**Examples**

Selecting the first two rows from the table from S3 file `https://storage.yandexcloud.net/my-test-bucket-768/data.csv`:

``` sql
SELECT *
FROM s3('https://storage.yandexcloud.net/my-test-bucket-768/data.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

The similar but from file with `gzip` compression:

``` sql
SELECT *
FROM s3('https://storage.yandexcloud.net/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

## Usage {#usage-examples}

Suppose that we have several files with following URIs on S3:

-   'https://storage.yandexcloud.net/my-test-bucket-768/some_prefix/some_file_1.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/some_prefix/some_file_2.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/some_prefix/some_file_3.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/some_prefix/some_file_4.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/another_prefix/some_file_1.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/another_prefix/some_file_2.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/another_prefix/some_file_3.csv'
-   'https://storage.yandexcloud.net/my-test-bucket-768/another_prefix/some_file_4.csv'

Count the amount of rows in files ending with numbers from 1 to 3:

``` sql
SELECT count(*)
FROM s3('https://storage.yandexcloud.net/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      18 │
└─────────┘
```

Count the total amount of rows in all files in these two directories:

``` sql
SELECT count(*)
FROM s3('https://storage.yandexcloud.net/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'name String, value UInt32')
```

``` text
┌─count()─┐
│      24 │
└─────────┘
```

!!! warning "Warning"
    If your listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.

Count the total amount of rows in files named `file-000.csv`, `file-001.csv`, … , `file-999.csv`:

``` sql
SELECT count(*)
FROM s3('https://storage.yandexcloud.net/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

``` text
┌─count()─┐
│      12 │
└─────────┘
```

Insert data into file `test-data.csv.gz`:

``` sql
INSERT INTO FUNCTION s3('https://storage.yandexcloud.net/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Insert data into file `test-data.csv.gz` from existing table:

``` sql
INSERT INTO FUNCTION s3('https://storage.yandexcloud.net/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

**See Also**

-   [S3 engine](../../engines/table-engines/integrations/s3.md)

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/s3/) <!--hide-->

)";

}

}

#endif
