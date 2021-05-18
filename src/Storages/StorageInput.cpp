#include <Storages/StorageInput.h>
#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <DataStreams/IBlockInputStream.h>
#include <memory>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>


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


class StorageInputSource : public SourceWithProgress, WithContext
{
public:
    StorageInputSource(ContextPtr context_, Block sample_block) : SourceWithProgress(std::move(sample_block)), WithContext(context_) {}

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


void StorageInput::setInputStream(BlockInputStreamPtr input_stream_)
{
    input_stream = input_stream_;
}


Pipe StorageInput::read(
    const Names & /*column_names*/,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    Pipes pipes;
    auto query_context = context->getQueryContext();
    /// It is TCP request if we have callbacks for input().
    if (query_context->getInputBlocksReaderCallback())
    {
        /// Send structure to the client.
        query_context->initializeInput(shared_from_this());
        return Pipe(std::make_shared<StorageInputSource>(query_context, metadata_snapshot->getSampleBlock()));
    }

    if (!input_stream)
        throw Exception("Input stream is not initialized, input() must be used only in INSERT SELECT query", ErrorCodes::INVALID_USAGE_OF_INPUT);

    return Pipe(std::make_shared<SourceFromInputStream>(input_stream));
}

}
