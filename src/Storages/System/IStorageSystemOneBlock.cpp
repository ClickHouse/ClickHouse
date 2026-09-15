#include <Storages/System/IStorageSystemOneBlock.h>
// #include <Core/NamesAndAliases.h>
// #include <DataTypes/DataTypeString.h>
// #include <Storages/ColumnsDescription.h>
// #include <Storages/IStorage.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/ISource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <functional>

namespace DB
{

namespace
{

/// A subquery set is only filled by `CreatingSetsStep`, which runs after the pipeline starts; sets of
/// every other kind are ready as soon as they are constructed.
bool dagHasUnbuiltSubquerySet(const ActionsDAG & dag)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN)
        {
            const ColumnSet * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
            if (column_set)
            {
                auto future_set = column_set->getData();
                if (!future_set->get() && typeid_cast<FutureSetFromSubquery *>(future_set.get()))
                    return true;
            }
        }

        /// `splitFilterNodeForAllowedInputs` keeps `indexHint` arguments, and their sets live in a
        /// separate DAG that this one only references through the function object.
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
        {
            if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get()))
                if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                    if (dagHasUnbuiltSubquerySet(index_hint->getActions()))
                        return true;
        }
    }

    return false;
}

class SystemOneBlockLazySource : public ISource
{
public:
    using FillFunc = std::function<void(MutableColumns &, const ActionsDAG::Node *, std::vector<UInt8>)>;

    SystemOneBlockLazySource(SharedHeader header_, FillFunc fill_, ActionsDAG filter_, std::vector<UInt8> columns_mask_)
        : ISource(std::move(header_))
        , fill(std::move(fill_))
        , filter(std::move(filter_))
        , columns_mask(std::move(columns_mask_))
    {
    }

    String getName() const override { return "SystemOneBlockLazy"; }

protected:
    Chunk generate() override
    {
        if (generated)
            return {};
        generated = true;

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        fill(res_columns, filter.getOutputs().at(0), std::move(columns_mask));

        UInt64 num_rows = res_columns.at(0)->size();
        if (num_rows == 0)
            return {};

        return Chunk(std::move(res_columns), num_rows);
    }

private:
    FillFunc fill;
    ActionsDAG filter;
    std::vector<UInt8> columns_mask;
    bool generated = false;
};

}

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
        SharedHeader sample_block,
        std::shared_ptr<IStorageSystemOneBlock> storage_,
        std::vector<UInt8> columns_mask_)
        : SourceStepWithFilter(
            std::move(sample_block),
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

void IStorageSystemOneBlock::readImpl(
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
    Block sample_block = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
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
        std::move(context), std::make_shared<const Block>(std::move(sample_block)), std::move(this_ptr), std::move(columns_mask));

    query_plan.addStep(std::move(reading));
}

void ReadFromSystemOneBlock::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto sample_block = getOutputHeader();

    storage->checkAccessRights(context);

    if (filter && dagHasUnbuiltSubquerySet(*filter))
    {
        /// `CreatingSetsStep` holds back every output of this pipeline until it has filled the sets, so
        /// a predicate that needs one of them can be evaluated from a source but not from here.
        auto fill = [source_storage = storage, source_context = context](
                        MutableColumns & columns, const ActionsDAG::Node * predicate, std::vector<UInt8> mask)
        { source_storage->fillData(columns, source_context, predicate, std::move(mask)); };

        pipeline.init(Pipe(std::make_shared<SystemOneBlockLazySource>(
            sample_block, std::move(fill), std::move(*filter), std::move(columns_mask))));
        return;
    }

    MutableColumns res_columns = sample_block->cloneEmptyColumns();
    const ActionsDAG::Node * predicate = filter ? filter->getOutputs().at(0) : nullptr;
    storage->fillData(res_columns, context, predicate, std::move(columns_mask));

    UInt64 num_rows = res_columns.at(0)->size();

    Chunk chunk;
    if (num_rows > 0)
        chunk = Chunk(std::move(res_columns), num_rows);

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

    filter = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &sample, context);
}

VirtualColumnsDescription IStorageSystemOneBlock::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

}
