#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const StorageObjectStorageTableOptions::Path & StorageObjectStorageTableOptions::getPathForRead(const Path & raw_path) const
{
    if (read_path.path.empty())
        return raw_path;
    return read_path;
}

void StorageObjectStorageTableOptions::setPathForRead(const Path & path)
{
    read_path = path;
}

StorageObjectStorageTableOptions::Path StorageObjectStorageTableOptions::getPathForWrite(const Path & raw_path, const std::string & partition_id) const
{
    auto path = raw_path;

    if (!schema_hash.empty())
        boost::replace_all(path.path, ObjectStorageConnectionConfiguration::SCHEMA_HASH_WILDCARD, schema_hash);

    if (!partition_strategy)
        return path;

    return Path{partition_strategy->getPathForWrite(path.path, partition_id)};
}

String StorageObjectStorageTableOptions::computeSchemaHash(const ColumnsDescription & columns)
{
    SipHash hash;
    auto columns_str = columns.getAllPhysical().toString();
    hash.update(columns_str.data(), columns_str.size());
    return getSipHash128AsHexString(hash);
}

void StorageObjectStorageTableOptions::setSchemaHash(const String & hash, Paths & paths)
{
    schema_hash = hash;
    boost::replace_all(read_path.path, ObjectStorageConnectionConfiguration::SCHEMA_HASH_WILDCARD, schema_hash);

    if (paths.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected exactly one path when setting schema hash, got {}", paths.size());
    boost::replace_all(paths[0].path, ObjectStorageConnectionConfiguration::SCHEMA_HASH_WILDCARD, schema_hash);
}

void StorageObjectStorageTableOptions::initPartitionStrategy(
    ASTPtr partition_by, const ColumnsDescription & columns, ContextPtr context, const Path & raw_path)
{
    partition_strategy = PartitionStrategyFactory::get(
        partition_strategy_type,
        partition_by,
        columns.getOrdinary(),
        context,
        format,
        raw_path.hasGlobsIgnorePlaceholders(),
        raw_path.hasPartitionWildcard(),
        partition_columns_in_data_file);

    if (partition_strategy)
    {
        read_path = partition_strategy->getPathForRead(raw_path.path);
        LOG_DEBUG(getLogger("StorageObjectStorageTableOptions"), "Initialized partition strategy {}", magic_enum::enum_name(partition_strategy_type));
    }
}

StorageObjectStorageTableOptions tableOptionsFromParsedArguments(StorageParsedArguments && parsed_arguments)
{
    StorageObjectStorageTableOptions table_options;
    table_options.format = std::move(parsed_arguments.format);
    table_options.compression_method = std::move(parsed_arguments.compression_method);
    table_options.structure = std::move(parsed_arguments.structure);
    table_options.partition_strategy_type = parsed_arguments.partition_strategy_type;
    table_options.partition_columns_in_data_file = parsed_arguments.partition_columns_in_data_file;
    table_options.partition_strategy = std::move(parsed_arguments.partition_strategy);
    return table_options;
}

}
