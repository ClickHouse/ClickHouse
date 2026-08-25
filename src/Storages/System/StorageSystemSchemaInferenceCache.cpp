#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageURL.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Formats/ReadSchemaUtils.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>

namespace DB
{

static String getSchemaString(const ColumnsDescription & columns)
{
    WriteBufferFromOwnString buf;
    const auto & names_and_types = columns.getAll();
    for (auto it = names_and_types.begin(); it != names_and_types.end(); ++it)
    {
        if (it != names_and_types.begin())
            writeCString(", ", buf);
        writeString(it->name, buf);
        writeChar(' ', buf);
        writeString(it->type->getName(), buf);
    }

    return buf.str();
}

ColumnsDescription StorageSystemSchemaInferenceCache::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"storage", std::make_shared<DataTypeString>(), "Storage name: File, URL, S3 or HDFS."},
        {"source", std::make_shared<DataTypeString>(), "File source."},
        {"format", std::make_shared<DataTypeString>(), "Format name."},
        {"additional_format_info", std::make_shared<DataTypeString>(),
            "Additional information required to identify the schema. For example, format specific settings."
        },
        {"registration_time", std::make_shared<DataTypeDateTime>(), "Timestamp when schema was added in cache."},
        {"schema", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Cached schema."},
        {"number_of_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Number of rows in the file in given format. It's used for caching trivial count() from data files and for caching number of rows from the metadata during schema inference."},
        {"schema_inference_mode", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Scheme inference mode."},
    };
}


static void fillDataImpl(MutableColumns & res_columns, SchemaCache & schema_cache, const String & storage_name)
{
    auto s3_schema_cache_data = schema_cache.getAll();

    for (const auto & [key, schema_info] : s3_schema_cache_data)
    {
        res_columns[0]->insert(storage_name);
        res_columns[1]->insert(key.source);
        res_columns[2]->insert(key.format);
        res_columns[3]->insert(key.additional_format_info);
        res_columns[4]->insert(schema_info.registration_time);
        if (schema_info.columns)
            res_columns[5]->insert(getSchemaString(*schema_info.columns));
        else
            res_columns[5]->insertDefault();
        if (schema_info.num_rows)
            res_columns[6]->insert(*schema_info.num_rows);
        else
            res_columns[6]->insertDefault();
        res_columns[7]->insert(key.schema_inference_mode);
    }
}

void StorageSystemSchemaInferenceCache::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    fillDataImpl(res_columns, StorageFile::getSchemaCache(context), "File");
#if USE_AWS_S3
    fillDataImpl(res_columns, StorageObjectStorage::getSchemaCache(context, StorageS3Configuration::type_name), "S3");
#endif
#if USE_HDFS
    fillDataImpl(res_columns, StorageObjectStorage::getSchemaCache(context, StorageHDFSConfiguration::type_name), "HDFS");
#endif
    fillDataImpl(res_columns, StorageURL::getSchemaCache(context), "URL");
#if USE_AZURE_BLOB_STORAGE
    fillDataImpl(res_columns, StorageObjectStorage::getSchemaCache(context, StorageAzureConfiguration::type_name), "Azure");
#endif
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSchemaInferenceCache) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "schema_inference_cache",
    .description = R"DOCS_MD(
Contains information about all cached file schemas.
)DOCS_MD",
    .examples = R"DOCS_MD(
Let's say we have a file `data.jsonl` with this content:
```json
{"id" :  1, "age" :  25, "name" :  "Josh", "hobbies" :  ["football", "cooking", "music"]}
{"id" :  2, "age" :  19, "name" :  "Alan", "hobbies" :  ["tennis", "art"]}
{"id" :  3, "age" :  32, "name" :  "Lana", "hobbies" :  ["fitness", "reading", "shopping"]}
{"id" :  4, "age" :  47, "name" :  "Brayan", "hobbies" :  ["movies", "skydiving"]}
```

<Tip>
Place `data.jsonl` in the `user_files_path` directory.  You can find this by looking
in your ClickHouse configuration files. The default is:
```sql
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
</Tip>

Open `clickhouse-client` and run the `DESCRIBE` query:

```sql
DESCRIBE file('data.jsonl') SETTINGS input_format_try_infer_integers=0;
```

```response
┌─name────┬─type────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id      │ Nullable(Float64)       │              │                    │         │                  │                │
│ age     │ Nullable(Float64)       │              │                    │         │                  │                │
│ name    │ Nullable(String)        │              │                    │         │                  │                │
│ hobbies │ Array(Nullable(String)) │              │                    │         │                  │                │
└─────────┴─────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Let's see the content of the `system.schema_inference_cache` table:

```sql
SELECT *
FROM system.schema_inference_cache
FORMAT Vertical
```
```response
Row 1:
──────
storage:                File
source:                 /home/droscigno/user_files/data.jsonl
format:                 JSONEachRow
additional_format_info: schema_inference_hints=, max_rows_to_read_for_schema_inference=25000, schema_inference_make_columns_nullable=true, try_infer_integers=false, try_infer_dates=true, try_infer_datetimes=true, try_infer_numbers_from_strings=true, read_bools_as_numbers=true, try_infer_objects=false
registration_time:      2022-12-29 17:49:52
schema:                 id Nullable(Float64), age Nullable(Float64), name Nullable(String), hobbies Array(Nullable(String))
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Automatic schema inference from input data](/concepts/features/interfaces/schema-inference)
)DOCS_MD")

}
