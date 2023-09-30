#include <Storages/StorageDummy.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/Chunk.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

StorageDummy::StorageDummy(const StorageID & table_id_, const ColumnsDescription & columns_, ColumnsDescription object_columns_)
    : IStorage(table_id_)
    , object_columns(std::move(object_columns_))
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
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t,
    size_t)
{
    query_plan.addStep(std::make_unique<ReadFromDummy>(*this, storage_snapshot, column_names));
}

ReadFromDummy::ReadFromDummy(const StorageDummy & storage_,
    StorageSnapshotPtr storage_snapshot_,
    Names column_names_)
    : SourceStepWithFilter(DataStream{.header = storage_snapshot_->getSampleBlockForColumns(column_names_)})
    , storage(storage_)
    , storage_snapshot(std::move(storage_snapshot_))
    , column_names(std::move(column_names_))
{}

void ReadFromDummy::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipe pipe(std::make_shared<SourceFromSingleChunk>(getOutputStream().header));
    pipeline.init(std::move(pipe));
}

}
