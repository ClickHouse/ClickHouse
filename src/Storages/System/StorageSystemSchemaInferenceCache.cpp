#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/StorageAzureBlob.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Formats/ReadSchemaUtils.h>

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

NamesAndTypesList StorageSystemSchemaInferenceCache::getNamesAndTypes()
{
    return {
        {"storage", std::make_shared<DataTypeString>()},
        {"source", std::make_shared<DataTypeString>()},
        {"format", std::make_shared<DataTypeString>()},
        {"additional_format_info", std::make_shared<DataTypeString>()},
        {"registration_time", std::make_shared<DataTypeDateTime>()},
        {"schema", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"number_of_rows", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())}
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
    }
}

void StorageSystemSchemaInferenceCache::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    fillDataImpl(res_columns, StorageFile::getSchemaCache(context), "File");
#if USE_AWS_S3
    fillDataImpl(res_columns, StorageS3::getSchemaCache(context), "S3");
#endif
#if USE_HDFS
    fillDataImpl(res_columns, StorageHDFS::getSchemaCache(context), "HDFS");
#endif
    fillDataImpl(res_columns, StorageURL::getSchemaCache(context), "URL");
#if USE_AZURE_BLOB_STORAGE
    fillDataImpl(res_columns, StorageAzureBlob::getSchemaCache(context), "Azure");
#endif
}

}
