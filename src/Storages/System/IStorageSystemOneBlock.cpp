#include <Storages/System/IStorageSystemOneBlock.h>
// #include <Core/NamesAndAliases.h>
// #include <DataTypes/DataTypeString.h>
// #include <Storages/ColumnsDescription.h>
// #include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

class ReadFromSystemOneBlock : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemOneBlock"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromSystemOneBlock(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        std::shared_ptr<IStorageSystemOneBlock> storage_,
        std::vector<UInt8> columns_mask_)
        : SourceStepWithFilter(
            DataStream{.header = std::move(sample_block)},
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , columns_mask(std::move(columns_mask_))
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<IStorageSystemOneBlock> storage;
    std::vector<UInt8> columns_mask;
    std::optional<ActionsDAG> filter;
};

void IStorageSystemOneBlock::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    std::vector<UInt8> columns_mask;

    if (supportsColumnsMask())
    {
        auto [columns_mask_, header] = getQueriedColumnsMaskAndHeader(sample_block, column_names);
        columns_mask = std::move(columns_mask_);
        sample_block = std::move(header);
    }

    auto this_ptr = std::static_pointer_cast<IStorageSystemOneBlock>(shared_from_this());

    auto reading = std::make_unique<ReadFromSystemOneBlock>(
        column_names, query_info, storage_snapshot,
        std::move(context), std::move(sample_block), std::move(this_ptr), std::move(columns_mask));

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemOneBlock::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const Block & sample_block = getOutputStream().header;
    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    const ActionsDAG::Node * predicate = filter ? filter->getOutputs().at(0) : nullptr;
    storage->fillData(res_columns, context, predicate, std::move(columns_mask));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    pipeline.init(Pipe(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk))));
}

void ReadFromSystemOneBlock::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (!filter_actions_dag)
        return;

    Block sample = storage->getFilterSampleBlock();
    if (sample.columns() == 0)
        return;

    filter = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &sample);

    /// Must prepare sets here, initializePipeline() would be too late, see comment on FutureSetFromSubquery.
    if (filter)
        VirtualColumnUtils::buildSetsForDAG(*filter, context);
}

}
