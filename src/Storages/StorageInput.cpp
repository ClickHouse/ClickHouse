#include <Storages/StorageInput.h>
#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <memory>
#include <Processors/ISource.h>
#include <Processors/Sources/ThrowingExceptionSource.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int LOGICAL_ERROR;
}

StorageInput::StorageInput(const StorageID & table_id, const ColumnsDescription & columns_)
    : IStorage(table_id)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}


class StorageInputSource : public ISource, WithContext
{
public:
    StorageInputSource(ContextPtr context_, Block sample_block) : ISource(std::move(sample_block)), WithContext(context_) {}

    Chunk generate() override
    {
        auto block = getContext()->getInputBlocksReaderCallback()(getContext());
        if (!block)
            return {};

        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    String getName() const override { return "Input"; }
};


void StorageInput::setPipe(Pipe pipe_)
{
    pipe = std::move(pipe_);
    was_pipe_initialized = true;
}

class ReadFromInput : public ISourceStep
{
public:
    std::string getName() const override { return "ReadFromInput"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromInput(
        Block sample_block,
        Pipe pipe_,
        StorageInput & storage_)
        : ISourceStep(std::move(sample_block))
        , pipe(std::move(pipe_))
        , storage(storage_)
    {
    }

private:
    Pipe pipe;
    StorageInput & storage;
};

void StorageInput::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    Pipe input_source_pipe;

    auto query_context = context->getQueryContext();
    /// It is TCP request if we have callbacks for input().
    if (query_context->getInputBlocksReaderCallback())
    {
        /// Send structure to the client.
        query_context->initializeInput(shared_from_this());
        input_source_pipe = Pipe(std::make_shared<StorageInputSource>(query_context, sample_block));
    }

    auto reading = std::make_unique<ReadFromInput>(
        std::move(sample_block),
        std::move(input_source_pipe),
        *this);

    query_plan.addStep(std::move(reading));
}

void ReadFromInput::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!pipe.empty())
    {
        pipeline.init(std::move(pipe));
        return;
    }

    if (!storage.was_pipe_initialized)
        throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "Input stream is not initialized, input() must be used only in INSERT SELECT query");

    if (storage.was_pipe_used)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to read from input() twice.");

    pipeline.init(std::move(storage.pipe));
    storage.was_pipe_used = true;
}

}
