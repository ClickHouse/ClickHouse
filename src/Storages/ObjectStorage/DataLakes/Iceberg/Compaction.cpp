#include <memory>
#include <string>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Logger.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

#if USE_AVRO

namespace DB::Iceberg
{

using namespace DB;

std::vector<String> getOldFiles(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration)
{
    auto metadata_files = listFiles(*object_storage, *configuration, "metadata", "");
    auto data_files = listFiles(*object_storage, *configuration, "data", "");

    for (auto && data_file : data_files)
        metadata_files.push_back(data_file);

    return metadata_files;
}

void clearOldFiles(ObjectStoragePtr object_storage, const std::vector<String> & old_files)
{
    for (const auto & metadata_file : old_files)
    {
        object_storage->removeObjectIfExists(StoredObject(metadata_file));
    }
}

void compactIcebergTable(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    const StorageID & table_id_)
{
    auto old_files = getOldFiles(object_storage_, configuration_);

    auto sink = std::make_shared<IcebergStorageSink>(object_storage_, configuration_, format_settings_, sample_block_, context_, catalog_, table_id_, false);

    auto columns_description = ColumnsDescription();
    for (size_t i = 0; i < sample_block_->columns(); ++i)
        columns_description.add(ColumnDescription(sample_block_->getNames()[i], sample_block_->getDataTypes()[i]));
    auto source = std::make_shared<StorageObjectStorage>(
        configuration_,
        object_storage_,
        context_,
        table_id_,
        /* columns */columns_description,
        /* constraints */ConstraintsDescription{},
        /* comment */"",
        format_settings_,
        LoadingStrictnessLevel::CREATE,
        catalog_,
        /* if_not_exists*/true,
        /* is_datalake_query*/true,
        /* distributed_processing */false,
        /* partition_by */nullptr,
        /* is_table_function */false,
        /* lazy_init */true);

    SelectQueryInfo query_info;
    auto metadata_snapshot = source->getInMemoryMetadataPtr();
    auto [_, iceberg_snapshot] = static_cast<IcebergMetadata*>(configuration_->getMetadata().get())->getRelevantState(context_);
    metadata_snapshot->setDataLakeTableState(iceberg_snapshot);
    auto storage_snapshot = source->getStorageSnapshot(metadata_snapshot, context_);
    QueryProcessingStage::Enum read_from_table_stage = source->getQueryProcessingStage(
        context_, QueryProcessingStage::Complete, storage_snapshot, query_info);

    QueryPlan plan;
    source->read(
        plan,
        metadata_snapshot->getColumns().getNamesOfPhysical(),
        storage_snapshot, query_info, context_,
        read_from_table_stage, DEFAULT_BLOCK_SIZE, 1);

    auto builder = plan.buildQueryPipeline(QueryPlanOptimizationSettings(context_), BuildQueryPipelineSettings(context_));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(sink);

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    clearOldFiles(object_storage_, old_files);
}

}

#endif
