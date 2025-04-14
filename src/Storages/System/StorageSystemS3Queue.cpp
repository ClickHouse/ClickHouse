#include "StorageSystemS3Queue.h"

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

ColumnsDescription StorageSystemS3Queue::getColumnsDescription()
{
    /// TODO: Fill in all the comments
    return ColumnsDescription
    {
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path in zookeeper to S3Queue metadata"},
        {"file_path", std::make_shared<DataTypeString>(), "File path of a file which is being processed by S3Queue"},
        {"file_name", std::make_shared<DataTypeString>(), "File name of a file which is being processed by S3Queue"},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Currently processed number of rows"},
        {"status", std::make_shared<DataTypeString>(), "Status of processing: Processed, Processing, Failed"},
        {"processing_start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file started"},
        {"processing_end_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time at which processing of the file ended"},
        {"exception", std::make_shared<DataTypeString>(), "Exception which happened during processing"},
    };
}

StorageSystemS3Queue::StorageSystemS3Queue(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemS3Queue::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [zookeeper_path, metadata] : ObjectStorageQueueMetadataFactory::instance().getAll())
    {
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

}
