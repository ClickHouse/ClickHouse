#include <Storages/System/StorageSystemOne.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    /// This column doesn't have a comment, because otherwise it will be added to all tables created via:
    /// CREATE TABLE test (dummy UInt8) ENGINE = Distributed(`default`, `system.one`)
    storage_metadata.setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
    setInMemoryMetadata(storage_metadata);
}


void StorageSystemOne::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    query_plan.addStep(std::make_unique<ReadFromSystemOneStep>(column_names, storage_snapshot));
}


ReadFromSystemOneStep::ReadFromSystemOneStep(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot_
)
    : ISourceStep(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)))
{
}


void ReadFromSystemOneStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto column = DataTypeUInt8().createColumnConst(1, 0u)->convertToFullColumnIfConst();
    Chunk chunk({ std::move(column) }, 1);

    auto source = std::make_shared<SourceFromSingleChunk>(getOutputHeader(), std::move(chunk));
    source->addTotalRowsApprox(1);

    pipeline.init(Pipe(source));
}

}
