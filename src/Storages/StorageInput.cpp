#include <Storages/StorageInput.h>
#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <memory>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
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
        //StorageSnapshotPtr storage_snapshot_,
        StorageInput & storage_)
        : ISourceStep(DataStream{.header = std::move(sample_block)})
        //, storage_snapshot(std::move(storage_snapshot_))
        , storage(storage_)
    {
    }

private:
    //StorageSnapshotPtr storage_snapshot;
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

    auto query_context = context->getQueryContext();
    /// It is TCP request if we have callbacks for input().
    if (!was_pipe_initialized && query_context->getInputBlocksReaderCallback())
    {
        /// Send structure to the client.
        query_context->initializeInput(shared_from_this());
    }

    if (!was_pipe_initialized)
        throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "Input stream is not initialized, input() must be used only in INSERT SELECT query");

    auto reading = std::make_unique<ReadFromInput>(
        std::move(sample_block),
        //storage_snapshot,
        *this);

    query_plan.addStep(std::move(reading));
}

void ReadFromInput::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (storage.was_pipe_used)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to read from input() twice.");

    pipeline.init(std::move(storage.pipe));
    storage.was_pipe_used = true;
}

}
