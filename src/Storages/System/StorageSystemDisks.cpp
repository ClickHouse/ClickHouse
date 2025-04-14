#include <Storages/System/StorageSystemDisks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/FileCacheFactory.h>

namespace DB
{

namespace ErrorCodes
{
}


StorageSystemDisks::StorageSystemDisks(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"name", std::make_shared<DataTypeString>(), "Name of a disk in the server configuration."},
        {"path", std::make_shared<DataTypeString>(), "Path to the mount point in the file system."},
        {"free_space", std::make_shared<DataTypeUInt64>(), "Free space on disk in bytes."},
        {"total_space", std::make_shared<DataTypeUInt64>(), "Disk volume in bytes."},
        {"unreserved_space", std::make_shared<DataTypeUInt64>(), "Free space which is not taken by reservations (free_space minus the size of reservations taken by merges, inserts, and other disk write operations currently running)."},
        {"keep_free_space", std::make_shared<DataTypeUInt64>(), "Amount of disk space that should stay free on disk in bytes. Defined in the keep_free_space_bytes parameter of disk configuration."},
        {"type", std::make_shared<DataTypeString>(), "The disk type which tells where this disk stores the data - RAM, local drive or remote storage."},
        {"object_storage_type", std::make_shared<DataTypeString>(), "Type of object storage if disk type is object_storage"},
        {"metadata_type", std::make_shared<DataTypeString>(), "Type of metadata storage if disk type is object_storage"},
        {"is_encrypted", std::make_shared<DataTypeUInt8>(), "Flag which shows whether this disk ecrypts the underlying data. "},
        {"is_read_only", std::make_shared<DataTypeUInt8>(), "Flag which indicates that you can only perform read operations with this disk."},
        {"is_write_once", std::make_shared<DataTypeUInt8>(), "Flag which indicates if disk is write-once. Which means that it does support BACKUP to this disk, but does not support INSERT into MergeTree table on this disk."},
        {"is_remote", std::make_shared<DataTypeUInt8>(), "Flag which indicated what operations with this disk involve network interaction."},
        {"is_broken", std::make_shared<DataTypeUInt8>(), "Flag which indicates if disk is broken. Broken disks will have 0 space and cannot be used."},
        {"cache_path", std::make_shared<DataTypeString>(), "The path to the cache directory on local drive in case when the disk supports caching."},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDisks::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_name = ColumnString::create();
    MutableColumnPtr col_path = ColumnString::create();
    MutableColumnPtr col_free = ColumnUInt64::create();
    MutableColumnPtr col_total = ColumnUInt64::create();
    MutableColumnPtr col_unreserved = ColumnUInt64::create();
    MutableColumnPtr col_keep = ColumnUInt64::create();
    MutableColumnPtr col_type = ColumnString::create();
    MutableColumnPtr col_object_storage_type = ColumnString::create();
    MutableColumnPtr col_metadata_type = ColumnString::create();
    MutableColumnPtr col_is_encrypted = ColumnUInt8::create();
    MutableColumnPtr col_is_read_only = ColumnUInt8::create();
    MutableColumnPtr col_is_write_once = ColumnUInt8::create();
    MutableColumnPtr col_is_remote = ColumnUInt8::create();
    MutableColumnPtr col_is_broken = ColumnUInt8::create();
    MutableColumnPtr col_cache_path = ColumnString::create();

    for (const auto & [disk_name, disk_ptr] : context->getDisksMap())
    {
        col_name->insert(disk_name);
        col_path->insert(disk_ptr->getPath());
        col_free->insert(disk_ptr->getAvailableSpace().value_or(std::numeric_limits<UInt64>::max()));
        col_total->insert(disk_ptr->getTotalSpace().value_or(std::numeric_limits<UInt64>::max()));
        col_unreserved->insert(disk_ptr->getUnreservedSpace().value_or(std::numeric_limits<UInt64>::max()));
        col_keep->insert(disk_ptr->getKeepingFreeSpace());
        auto data_source_description = disk_ptr->getDataSourceDescription();
        col_type->insert(magic_enum::enum_name(data_source_description.type));
        col_object_storage_type->insert(magic_enum::enum_name(data_source_description.object_storage_type));
        col_metadata_type->insert(magic_enum::enum_name(data_source_description.metadata_type));
        col_is_encrypted->insert(data_source_description.is_encrypted);
        col_is_read_only->insert(disk_ptr->isReadOnly());
        col_is_write_once->insert(disk_ptr->isWriteOnce());
        col_is_remote->insert(disk_ptr->isRemote());
        col_is_broken->insert(disk_ptr->isBroken());

        String cache_path;
        if (disk_ptr->supportsCache())
            cache_path = FileCacheFactory::instance().getByName(disk_ptr->getCacheName())->getSettings().base_path;

        col_cache_path->insert(cache_path);
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_name));
    res_columns.emplace_back(std::move(col_path));
    res_columns.emplace_back(std::move(col_free));
    res_columns.emplace_back(std::move(col_total));
    res_columns.emplace_back(std::move(col_unreserved));
    res_columns.emplace_back(std::move(col_keep));
    res_columns.emplace_back(std::move(col_type));
    res_columns.emplace_back(std::move(col_object_storage_type));
    res_columns.emplace_back(std::move(col_metadata_type));
    res_columns.emplace_back(std::move(col_is_encrypted));
    res_columns.emplace_back(std::move(col_is_read_only));
    res_columns.emplace_back(std::move(col_is_write_once));
    res_columns.emplace_back(std::move(col_is_remote));
    res_columns.emplace_back(std::move(col_is_broken));
    res_columns.emplace_back(std::move(col_cache_path));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
