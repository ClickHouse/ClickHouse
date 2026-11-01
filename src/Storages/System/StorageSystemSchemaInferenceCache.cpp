#include <Columns/IColumn.h>
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
#include <IO/ReadHelpers.h>
#include <Common/maskURIPassword.h>
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
        {"compression_method", std::make_shared<DataTypeString>(), "Compression method name."},
        {"headers", std::make_shared<DataTypeString>(), "Request headers used for URL storage."},
    };
}


static String redactHTTPHeadersForSystemTable(const String & headers)
{
    if (headers.empty())
        return "";

    String redacted;
    size_t pos = 0;
    while (pos < headers.size())
    {
        size_t colon_name = headers.find(':', pos);
        if (colon_name == String::npos)
            break;
        size_t name_len = parse<size_t>(headers.substr(pos, colon_name - pos));
        size_t name_start = colon_name + 1;
        if (name_start + name_len >= headers.size() || headers[name_start + name_len] != '=')
            break;
        std::string_view name(headers.data() + name_start, name_len);

        size_t val_len_start = name_start + name_len + 1;
        size_t colon_val = headers.find(':', val_len_start);
        if (colon_val == String::npos)
            break;
        size_t val_len = parse<size_t>(headers.substr(val_len_start, colon_val - val_len_start));
        size_t val_start = colon_val + 1;
        if (val_start + val_len >= headers.size() || headers[val_start + val_len] != ';')
            break;

        std::string_view hidden = "[HIDDEN]";
        redacted.append(std::to_string(name.size()));
        redacted.push_back(':');
        redacted.append(name);
        redacted.push_back('=');
        redacted.append(std::to_string(hidden.size()));
        redacted.push_back(':');
        redacted.append(hidden);
        redacted.push_back(';');

        pos = val_start + val_len + 1;
    }
    return redacted.empty() ? "[HIDDEN]" : redacted;
}

static void fillDataImpl(MutableColumns & res_columns, SchemaCache & schema_cache, const String & storage_name)
{
    auto s3_schema_cache_data = schema_cache.getAll();

    for (const auto & [key, schema_info] : s3_schema_cache_data)
    {
        res_columns[0]->insert(storage_name);
        String source = key.source;
        /// Only a URL can carry a credential. A `File` key is a local path, where the presigned
        /// parameter scan would rewrite a '?' in a file name and swallow everything after it.
        if (findURIAuthority(source) != String::npos)
            maskURICredentials(source);
        res_columns[1]->insert(source);
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
        res_columns[8]->insert(key.compression_method);
        res_columns[9]->insert(redactHTTPHeadersForSystemTable(key.headers));
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
