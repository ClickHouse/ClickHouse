#include "ReadFromMemoryStorageStep.h"

#include <atomic>
#include <functional>
#include <memory>

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

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    const size_t num_streams_,
    const bool delay_read_for_global_sub_queries_)
    : SourceStepWithFilter(
        DataStream{.header = storage_snapshot_->getSampleBlockForColumns(columns_to_read_)},
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
        assert(output_stream != std::nullopt);
        pipe = Pipe(std::make_shared<NullSource>(output_stream->header));
    }

    pipeline.init(std::move(pipe));
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

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
            columns_to_read,
            storage_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [my_storage = storage](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = assert_cast<const StorageMemory &>(*my_storage).data.get();
            }));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(columns_to_read, storage_snapshot, current_data, parallel_execution_index));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
