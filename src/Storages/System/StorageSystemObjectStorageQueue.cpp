#include <Storages/System/StorageSystemObjectStorageQueue.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadataFactory.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Disks/IDisk.h>


namespace DB
{

template <ObjectStorageType type>
ColumnsDescription StorageSystemObjectStorageQueue<type>::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path in zookeeper to metadata"},
        {"file_path", std::make_shared<DataTypeString>(), "File path of a file which is being processed"},
        {"file_name", std::make_shared<DataTypeString>(), "File name of a file which is being processed"},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Currently processed number of rows"},
        {"status", std::make_shared<DataTypeString>(), "Status of processing: Processed, Processing, Failed"},
        {"processing_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file started"},
        {"processing_end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file ended"},
        {"exception", std::make_shared<DataTypeString>(), "Exception which happened during processing"},
    };
}

template <ObjectStorageType type>
StorageSystemObjectStorageQueue<type>::StorageSystemObjectStorageQueue(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

template <ObjectStorageType type>
void StorageSystemObjectStorageQueue<type>::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [zookeeper_path, metadata] : ObjectStorageQueueMetadataFactory::instance().getAll())
    {
        if (type != metadata->getType())
            continue;

        for (const auto & [file_path, file_status] : metadata->getFileStatuses())
        {
            size_t i = 0;
            res_columns[i++]->insert(zookeeper_path);
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
        }
    }
}

template class StorageSystemObjectStorageQueue<ObjectStorageType::S3>;
template class StorageSystemObjectStorageQueue<ObjectStorageType::Azure>;

}
