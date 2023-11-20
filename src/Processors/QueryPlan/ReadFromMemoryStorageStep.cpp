#include "ReadFromMemoryStorageStep.h"

#include <atomic>
#include <functional>
#include <memory>

#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>

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

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(Pipe pipe_) :
    SourceStepWithFilter(DataStream{.header = pipe_.getHeader()}),
    pipe(std::move(pipe_))
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    // use move - make sure that the call will only be made once.
    pipeline.init(std::move(pipe));
}

Pipe ReadFromMemoryStorageStep::makePipe(const Names & columns_to_read_,
              const StorageSnapshotPtr & storage_snapshot_,
              size_t num_streams_,
              const bool delay_read_for_global_sub_queries_)
{
    storage_snapshot_->check(columns_to_read_);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot_->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries_)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            columns_to_read_,
            storage_snapshot_,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [current_data](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = current_data;
            }));
    }

    size_t size = current_data->size();

    if (num_streams_ > size)
        num_streams_ = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams_; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(columns_to_read_, storage_snapshot_, current_data, parallel_execution_index));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
