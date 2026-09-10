#include <Storages/System/StorageSystemObjectStorageQueueMetadataCache.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Disks/IDisk.h>


namespace DB
{

template <ObjectStorageType type>
ColumnsDescription StorageSystemObjectStorageQueueMetadataCache<type>::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path in zookeeper to metadata"},
        {"file_path", std::make_shared<DataTypeString>(), "File path of a file which is being processed"},
        {"file_name", std::make_shared<DataTypeString>(), "File name of a file which is being processed"},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Currently processed number of rows"},
        {"status", std::make_shared<DataTypeString>(), "Status of processing: Processed, Processing, Failed. "
            "A non-null `processing_by_another_server_time` means that the `Processing` status belongs to a processor of another server"},
        {"processing_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file started"},
        {"processing_end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file ended"},
        {"exception", std::make_shared<DataTypeString>(), "Exception which happened during processing"},
        {"processing_by_another_server_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Non-null if the `Processing` status was not set by a processor of this server, but observed in keeper, "
            "which means that the file is being processed elsewhere: the time of that observation. "
            "In this case `rows_processed`, `processing_start_time`, `processing_end_time` and `exception` "
            "describe the last attempt of this server to process the file, not the ongoing processing"},
    };
}

template <ObjectStorageType type>
StorageSystemObjectStorageQueueMetadataCache<type>::StorageSystemObjectStorageQueueMetadataCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

template <ObjectStorageType type>
void StorageSystemObjectStorageQueueMetadataCache<type>::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [zookeeper_path, metadata] : ObjectStorageQueueMetadataFactory::instance().getAll())
    {
        if (type != metadata->getType())
            continue;

        auto cache = metadata->getFileStatusesCache().dump();
        for (const auto & [_, file_status] : cache)
        {
            size_t i = 0;
            res_columns[i++]->insert(zookeeper_path);
            const auto file_path = file_status->path;
            res_columns[i++]->insert(file_path);
            res_columns[i++]->insert(std::filesystem::path(file_path).filename().string());

            res_columns[i++]->insert(file_status->processed_rows.load());
            res_columns[i++]->insert(magic_enum::enum_name(file_status->state.load()));

            if (file_status->processing_start_time)
                res_columns[i++]->insert(file_status->processing_start_time.load());
            else
                res_columns[i++]->insertDefault();
            if (file_status->processing_end_time)
                res_columns[i++]->insert(file_status->processing_end_time.load());
            else
                res_columns[i++]->insertDefault();

            res_columns[i++]->insert(file_status->getException());

            if (file_status->foreign_processing_time)
                res_columns[i++]->insert(file_status->foreign_processing_time.load());
            else
                res_columns[i++]->insertDefault();
        }
    }
}

template class StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::S3>;
template class StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::Azure>;

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::Azure>) }
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemObjectStorageQueueMetadataCache<ObjectStorageType::S3>) }
