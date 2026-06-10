#include <Storages/System/StorageSystemPlainRewritableDataPaths.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

StorageSystemPlainRewritableDataPaths::StorageSystemPlainRewritableDataPaths(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk_name", std::make_shared<DataTypeString>(), "Name of the plain_rewritable disk."},
        {"common_prefix_for_blobs", std::make_shared<DataTypeString>(), "Object storage key prefix common to all blobs of this disk."},
        {"local_path", std::make_shared<DataTypeString>(), "Logical directory path the file belongs to."},
        {"directory_remote_path", std::make_shared<DataTypeString>(), "Randomly-generated object storage directory backing the logical directory."},
        {"name", std::make_shared<DataTypeString>(), "File name inside the directory."},
        {"remote_path", std::make_shared<DataTypeString>(), "Full blob object key in object storage."},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the file in bytes."},
        {"last_modified", std::make_shared<DataTypeDateTime>(), "Last modification time of the blob."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemPlainRewritableDataPaths::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

Pipe StorageSystemPlainRewritableDataPaths::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_disk_name = ColumnString::create();
    MutableColumnPtr col_common_prefix = ColumnString::create();
    MutableColumnPtr col_local_path = ColumnString::create();
    MutableColumnPtr col_directory_remote_path = ColumnString::create();
    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_remote_path = ColumnString::create();
    MutableColumnPtr col_size = ColumnUInt64::create();
    MutableColumnPtr col_last_modified = ColumnUInt32::create();

    for (const auto & [disk_name, disk_ptr] : context->getDisksMap())
    {
        /// Filter by metadata type first: getMetadataStorage() is not implemented for plain local disks.
        if (disk_ptr->getDataSourceDescription().metadata_type != MetadataStorageType::PlainRewritable)
            continue;

        const auto metadata_storage = disk_ptr->getMetadataStorage();
        const auto * plain_rewritable = dynamic_cast<const MetadataStorageFromPlainRewritableObjectStorage *>(metadata_storage.get());
        if (!plain_rewritable)
            continue;

        const auto common_prefix = plain_rewritable->getCommonKeyPrefix();
        for (const auto & object : plain_rewritable->getAllRemoteObjects())
        {
            col_disk_name->insert(disk_name);
            col_common_prefix->insert(common_prefix);
            col_local_path->insert(object.local_path);
            col_directory_remote_path->insert(object.directory_remote_path);
            col_name->insert(object.file_name);
            col_remote_path->insert(object.remote_path);
            col_size->insert(object.size);
            col_last_modified->insert(static_cast<UInt32>(object.last_modified));
        }
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_disk_name));
    res_columns.emplace_back(std::move(col_common_prefix));
    res_columns.emplace_back(std::move(col_local_path));
    res_columns.emplace_back(std::move(col_directory_remote_path));
    res_columns.emplace_back(std::move(col_name));
    res_columns.emplace_back(std::move(col_remote_path));
    res_columns.emplace_back(std::move(col_size));
    res_columns.emplace_back(std::move(col_last_modified));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(storage_snapshot->metadata->getSampleBlock()), std::move(chunk)));
}

}
