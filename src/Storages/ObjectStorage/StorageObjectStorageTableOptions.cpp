#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

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
        boost::replace_all(path.path, StorageObjectStorageConfiguration::SCHEMA_HASH_WILDCARD, schema_hash);

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

}
