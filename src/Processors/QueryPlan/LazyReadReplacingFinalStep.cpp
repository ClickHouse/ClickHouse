#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/LazyReadReplacingFinalStep.h>
#include <Processors/Sources/LazyReadReplacingFinalSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

LazyReadReplacingFinalStep::LazyReadReplacingFinalStep(
    StorageMetadataPtr metadata_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeSettingsPtr data_settings_,
    const MergeTreeData & data_,
    PartitionIdToMaxBlockPtr max_block_numbers_to_read_,
    RangesInDataPartsPtr ranges_,
    ContextPtr query_context_,
    FutureSetPtr future_set_)
    : ISourceStep(std::make_shared<const Block>(Block({ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "__global_row_index"}})))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , storage_snapshot(std::move(storage_snapshot_))
    , data_settings(std::move(data_settings_))
    , data(data_)
    , max_block_numbers_to_read(std::move(max_block_numbers_to_read_))
    , ranges(std::move(ranges_))
    , query_context(std::move(query_context_))
    , future_set(std::move(future_set_))
{
}

void LazyReadReplacingFinalStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto source = std::make_shared<LazyReadReplacingFinalSource>(
        metadata_snapshot,
        mutations_snapshot,
        storage_snapshot,
        data_settings,
        data,
        max_block_numbers_to_read,
        ranges,
        query_context,
        future_set);

    pipeline.init(Pipe(std::move(source)));
}

}
