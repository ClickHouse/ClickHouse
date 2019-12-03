#include <Storages/StorageInput.h>
#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <DataStreams/IBlockInputStream.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
}

StorageInput::StorageInput(const String &table_name_, const ColumnsDescription & columns_)
    : IStorage(columns_), table_name(table_name_)
{
    setColumns(columns_);
}


class StorageInputBlockInputStream : public IBlockInputStream
{
public:
    StorageInputBlockInputStream(Context & context_, const Block sample_block_)
        : context(context_),
        sample_block(sample_block_)
    {
    }

    Block readImpl() override { return context.getInputBlocksReaderCallback()(context); }
    void readPrefix() override {}
    void readSuffix() override {}

    String getName() const override { return "Input"; }

    Block getHeader() const override { return sample_block; }

private:
    Context & context;
    const Block sample_block;
};


void StorageInput::setInputStream(BlockInputStreamPtr input_stream_)
{
    input_stream = input_stream_;
}


BlockInputStreams StorageInput::read(const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    Context & query_context = const_cast<Context &>(context).getQueryContext();
    /// It is TCP request if we have callbacks for input().
    if (query_context.getInputBlocksReaderCallback())
    {
        /// Send structure to the client.
        query_context.initializeInput(shared_from_this());
        input_stream = std::make_shared<StorageInputBlockInputStream>(query_context, getSampleBlock());
    }

    if (!input_stream)
        throw Exception("Input stream is not initialized, input() must be used only in INSERT SELECT query", ErrorCodes::INVALID_USAGE_OF_INPUT);

    return {input_stream};
}

}
