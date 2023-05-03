#include "ReadFromMemoryStorageStep.h"

#include <atomic>
#include <functional>
#include <memory>
#include <stack>

#include <Functions/IFunction.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;
public:

    MemorySource(
        Names column_names_,
        const StorageSnapshotPtr & storage_snapshot,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {})
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_))
        , column_names_and_types(storage_snapshot->getColumnsByNames(
              GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withExtendedObjects(), column_names_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            initializer_func(data);
            initializer_func = {};
        }

        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
        {
            return {};
        }

        const Block & src = (*data)[current_index];

        Columns columns;
        size_t num_columns = column_names_and_types.size();
        columns.reserve(num_columns);

        auto name_and_type = column_names_and_types.begin();
        for (size_t i = 0; i < num_columns; ++i)
        {
            columns.emplace_back(tryGetColumnFromBlock(src, *name_and_type));
            ++name_and_type;
        }

        fillMissingColumns(columns, src.rows(), column_names_and_types, column_names_and_types, {}, nullptr);
        assert(std::all_of(columns.begin(), columns.end(), [](const auto & column) { return column != nullptr; }));

        return Chunk(std::move(columns), src.rows());
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }
        else
        {
            return execution_index++;
        }
    }

    const NamesAndTypesList column_names_and_types;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(const Names & columns_to_read_,
                                                     const StorageSnapshotPtr & storage_snapshot_,
                                                     const size_t num_streams_,
                                                     const bool delay_read_for_global_sub_queries_) :
    SourceStepWithFilter(DataStream{.header=storage_snapshot_->getSampleBlockForColumns(columns_to_read_)}),
    columns_to_read(columns_to_read_),
    storage_snapshot(storage_snapshot_),
    num_streams(num_streams_),
    delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        assert(output_stream != std::nullopt);
        pipe = Pipe(std::make_shared<NullSource>(output_stream->header));
    }

    pipeline.init(std::move(pipe));
}

ReadFromMemoryStorageStep::FixedColumns ReadFromMemoryStorageStep::makeFixedColumns()
{
    FixedColumns fixed_columns;
    for (const auto * filter_expression : getFilterNodes().nodes)
    {
        if (!filter_expression)
        {
            continue;
        }

        std::stack<const ActionsDAG::Node *> stack;
        stack.push(filter_expression);

        while (!stack.empty())
        {
            const auto * node = stack.top();
            stack.pop();
            if (node->type == ActionsDAG::ActionType::FUNCTION)
            {
                const auto & func_name = node->function_base->getName();
                if (func_name == "and")
                {
                    for (const auto * arg : node->children)
                        stack.push(arg);
                }
                else if (func_name == "equals")
                {
                    const ActionsDAG::Node * maybe_fixed_column = nullptr;
                    size_t num_constant_columns = 0;
                    for (const auto & child : node->children)
                    {
                        if (child->column)
                            ++num_constant_columns;
                        else
                            maybe_fixed_column = child;
                    }
                    if (maybe_fixed_column && num_constant_columns + 1 == node->children.size())
                    {
                        fixed_columns.insert(maybe_fixed_column);
                    }
                }
            }
        }
    }
    return fixed_columns;
}

std::shared_ptr<const Blocks> ReadFromMemoryStorageStep::getBlocksWithNecessaryColumns(std::shared_ptr<const Blocks> blocks_ptr)
{
    if (!blocks_ptr || blocks_ptr->empty())
    {
        return blocks_ptr;
    }

    Blocks blocks_with_necessary_columns;
    blocks_with_necessary_columns.reserve(columns_to_read.size());

    for (const auto & block : *blocks_ptr)
    {
        Block block_with_necessary_columns;

        for (const auto & column_to_read : columns_to_read)
        {
            if (const auto * p = block.findByName(column_to_read); p != nullptr)
            {
                block_with_necessary_columns.insert(*p);
            }
        }
        if (block_with_necessary_columns)
        {
            blocks_with_necessary_columns.push_back(std::move(block_with_necessary_columns));
        }
    }
    return std::make_shared<const Blocks>(std::move(blocks_with_necessary_columns));
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto blocks_with_necessary_columns = getBlocksWithNecessaryColumns(snapshot_data.blocks);

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
            columns_to_read,
            storage_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [blocks_with_necessary_columns](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = blocks_with_necessary_columns;
            }));
    }

    size_t size = blocks_with_necessary_columns->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(columns_to_read, storage_snapshot, blocks_with_necessary_columns, parallel_execution_index));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
