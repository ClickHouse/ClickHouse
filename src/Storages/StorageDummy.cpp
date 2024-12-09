#include <Storages/StorageDummy.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

StorageDummy::StorageDummy(
    const StorageID & table_id_, const ColumnsDescription & columns_, const StorageSnapshotPtr & original_storage_snapshot_)
    : IStorage(table_id_), original_storage_snapshot(original_storage_snapshot_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum StorageDummy::getQueryProcessingStage(
    ContextPtr,
    QueryProcessingStage::Enum,
    const StorageSnapshotPtr &,
    SelectQueryInfo &) const
{
    return QueryProcessingStage::FetchColumns;
}

void StorageDummy::read(QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum,
    size_t,
    size_t)
{
    query_plan.addStep(std::make_unique<ReadFromDummy>(
        column_names,
        query_info,
        original_storage_snapshot ? original_storage_snapshot : storage_snapshot,
        local_context,
        *this));
}

ReadFromDummy::ReadFromDummy(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const StorageDummy & storage_)
    : SourceStepWithFilter(SourceStepWithFilter::applyPrewhereActions(
                storage_snapshot_->getSampleBlockForColumns(column_names_), query_info_.prewhere_info),
        column_names_,
        query_info_,
        storage_snapshot_,
        context_)
    , storage(storage_)
    , column_names(column_names_)
{
}

void ReadFromDummy::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipe pipe(std::make_shared<SourceFromSingleChunk>(getOutputHeader()));
    pipeline.init(std::move(pipe));
}

}
