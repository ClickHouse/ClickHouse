#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <atomic>
#include <functional>
#include <memory>

#include <Analyzer/TableNode.h>

#include <Common/typeid_cast.h>

#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;

    static Block getHeader(const NamesAndTypesList & physical, const NamesAndTypesList & virtuals)
    {
        Block res;
        for (const auto & name_type : physical)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        for (const auto & name_type : virtuals)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        return res;
    }

public:
    MemorySource(
        NamesAndTypesList physical_columns_,
        NamesAndTypesList virtual_columns_,
        StorageID storage_id_,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {},
        MaterializedCTEPtr materialized_cte_ = {})
        : ISource(std::make_shared<const Block>(getHeader(physical_columns_, virtual_columns_)))
        , physical_columns(std::move(physical_columns_))
        , virtual_columns(std::move(virtual_columns_))
        , storage_id(std::move(storage_id_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
        , materialized_cte(std::move(materialized_cte_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            if (materialized_cte && !materialized_cte->is_built)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Reading from materialized CTE '{}' before it has been materialized (materialization was planned: {})",
                    materialized_cte->cte_name,
                    materialized_cte->is_materialization_planned.load());

            initializer_func(data);
            initializer_func = {};
        }

        Columns columns;
        columns.reserve(physical_columns.size() + virtual_columns.size());
        fillPhysicalColumns(columns);

        UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
        if (!columns.empty())
            fillVirtualColumns(columns, num_rows);

        return Chunk(std::move(columns), num_rows);
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }

        return execution_index++;
    }

    void fillPhysicalColumns(Columns & result_columns)
    {
        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
            return;

        const Block & src = (*data)[current_index];

        for (const auto & name_and_type : physical_columns)
        {
            if (name_and_type.isSubcolumn())
                result_columns.emplace_back(tryGetSubcolumnFromBlock(src, name_and_type.getTypeInStorage(), name_and_type));
            else
                result_columns.emplace_back(tryGetColumnFromBlock(src, name_and_type));
        }

        fillMissingColumns(result_columns, src.rows(), physical_columns, physical_columns, {}, nullptr);
        assert(std::all_of(result_columns.begin(), result_columns.end(), [](const auto & column) { return column != nullptr; }));
    }

    void fillVirtualColumns(Columns & result_columns, UInt64 num_rows) const
    {
        for (const auto & col : virtual_columns)
        {
            if (col.name == "_table")
                result_columns.emplace_back(col.type->createColumnConst(num_rows, storage_id.getTableName())->convertToFullColumnIfConst());
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual column: '{}'", col.name);
        }
    }

    const NamesAndTypesList physical_columns;
    const NamesAndTypesList virtual_columns;
    const StorageID storage_id;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
    MaterializedCTEPtr materialized_cte;
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    const size_t num_streams_,
    const bool delay_read_for_global_sub_queries_)
    : SourceStepWithFilter(
        std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns_to_read_)),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , columns_to_read(columns_to_read_)
    , storage(std::move(storage_))
    , num_streams(num_streams_)
    , delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromMemoryStorageStep::clone() const
{
    return std::make_unique<ReadFromMemoryStorageStep>(*this);
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    auto [physical_column_names, virtual_column_names] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(columns_to_read, storage_snapshot);
    auto physical_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), physical_column_names);
    auto virtual_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(), virtual_column_names);
    auto storage_id = storage->getStorageID();

    /// Use temporary table name if storage is temporary.
    if (query_info.table_expression)
        if (auto * table_node = query_info.table_expression->as<TableNode>())
            if (table_node->isTemporaryTable())
                storage_id.table_name = table_node->getTemporaryTableName();

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            physical_columns,
            virtual_columns,
            storage_id,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [my_storage = storage](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = assert_cast<const StorageMemory &>(*my_storage).data.get();
            },
            typeid_cast<StorageMemory *>(storage.get())->getMaterializedCTE()));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto source = std::make_shared<MemorySource>(physical_columns, virtual_columns, storage_id, current_data, parallel_execution_index);
        if (stream == 0)
            source->addTotalRowsApprox(snapshot_data.rows_approx);
        pipes.emplace_back(std::move(source));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
