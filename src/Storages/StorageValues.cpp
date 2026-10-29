#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageValues.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Reads the prepared pipe of `StorageValues`, taking it from the storage only when the pipeline is built.
class ReadFromPreparedPipeStep final : public ISourceStep
{
public:
    ReadFromPreparedPipeStep(
        SharedHeader header_,
        std::shared_ptr<StorageValues> storage_,
        Names column_names_,
        StorageSnapshotPtr storage_snapshot_,
        ContextPtr context_)
        : ISourceStep(std::move(header_))
        , storage(std::move(storage_))
        , column_names(std::move(column_names_))
        , storage_snapshot(std::move(storage_snapshot_))
        , context(std::move(context_))
    {
    }

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto pipe = storage->takePreparedPipe(column_names, storage_snapshot, context);
        for (const auto & processor : pipe.getProcessors())
            processors.emplace_back(processor);

        /// Unlike `ReadFromStorageStep`, the read limits of the query are not set on the processors: they are the processors
        /// of the insert, and a limit in the `break` mode would cancel its sources and cut the insert short.
        pipeline.init(std::move(pipe));
    }

private:
    std::shared_ptr<StorageValues> storage;
    Names column_names;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
};

}

StorageValues::StorageValues(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    Block res_block_,
    VirtualColumnsDescription virtuals_)
    : IStorage(table_id_), res_block(std::move(res_block_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(std::move(virtuals_));
    setInMemoryMetadata(storage_metadata);
}

StorageValues::StorageValues(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    Pipe prepared_pipe_,
    VirtualColumnsDescription virtuals_)
    : IStorage(table_id_), prepared_pipe(std::move(prepared_pipe_)), has_prepared_pipe(!prepared_pipe.empty())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setVirtuals(std::move(virtuals_));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageValues::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    if (has_prepared_pipe)
    {
        checkPreparedPipeCanBeRead();
        return takePreparedPipe(column_names, storage_snapshot, context);
    }

    /// Get only required columns.
    Block block;
    for (const auto & name : column_names)
        block.insert(res_block.getColumnOrSubcolumnByName(name));

    Chunk chunk(block.getColumns(), block.rows());
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(block.cloneEmpty()), std::move(chunk)));
}

void StorageValues::checkPreparedPipeCanBeRead()
{
    if (!prepared_pipe_read_allowed)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "The data of table {} cannot be read while the query is analyzed, e.g. in a scalar subquery or in `EXISTS`",
            getStorageID().getNameForLogs());
    if (prepared_pipe_was_read)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The data of table {} can be read only once in a query", getStorageID().getNameForLogs());
    prepared_pipe_was_read = true;
}

void StorageValues::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    if (!has_prepared_pipe)
    {
        IStorage::read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
        return;
    }

    storage_snapshot->check(column_names);
    checkPreparedPipeCanBeRead();

    auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    query_plan.addStep(std::make_unique<ReadFromPreparedPipeStep>(
        std::move(header), std::static_pointer_cast<StorageValues>(shared_from_this()), column_names, storage_snapshot, context));
}

Pipe StorageValues::takePreparedPipe(const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr context)
{
    if (prepared_pipe_was_taken)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The prepared pipe of table {} is taken twice", getStorageID().getNameForLogs());
    prepared_pipe_was_taken = true;

    const auto & pipe_header = prepared_pipe.getHeader();
    ActionsDAG dag(pipe_header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.reserve(column_names.size());
    for (const auto & name : column_names)
    {
        if (pipe_header.has(name))
        {
            outputs.push_back(dag.getOutputs()[pipe_header.getPositionByName(name)]);
            continue;
        }

        /// The pipe contains only the physical columns, so the subcolumns are extracted from them.
        auto column = storage_snapshot->getColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), name);
        if (!column.isSubcolumn())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is not found in the data of table {}", name, getStorageID().getNameForLogs());

        const auto * column_node = dag.getOutputs()[pipe_header.getPositionByName(column.getNameInStorage())];
        auto subcolumn_name_type = std::make_shared<DataTypeString>();
        auto subcolumn_name_column = subcolumn_name_type->createColumnConst(0, column.getSubcolumnName());
        const auto & subcolumn_name_node = dag.addColumn(std::move(subcolumn_name_column), std::move(subcolumn_name_type), column.getSubcolumnName());

        auto get_subcolumn = FunctionFactory::instance().get("getSubcolumn", context);
        const auto & get_subcolumn_node = dag.addFunction(get_subcolumn, {column_node, &subcolumn_name_node}, {});
        outputs.push_back(&dag.addAlias(get_subcolumn_node, name));
    }

    dag.getOutputs().swap(outputs);
    auto expression = std::make_shared<ExpressionActions>(std::move(dag));

    prepared_pipe.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });

    return std::move(prepared_pipe);
}

std::optional<Pipe> StorageValues::extractPreparedPipe()
{
    if (!has_prepared_pipe || prepared_pipe_was_taken)
        return {};

    prepared_pipe_was_taken = true;
    return std::move(prepared_pipe);
}

}
