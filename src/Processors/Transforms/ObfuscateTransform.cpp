#include <Processors/Transforms/ObfuscateTransform.h>
#include <Processors/Port.h>

namespace DB
{

ObfuscateTransform::ObfuscateTransform(
    const Block & header_,
    TemporaryDataOnDiskScopePtr tmp_data_scope_,
    const MarkovModelParameters & params_,
    UInt64 seed_,
    bool keep_original_data_)
    : IAccumulatingTransform(std::make_shared<const Block>(header_), std::make_shared<const Block>(header_))
    , obfuscator(header_, seed_, params_)
    , stream_holder(std::make_shared<const Block>(header_.cloneEmpty()), std::move(tmp_data_scope_))
    , keep_original_data(keep_original_data_)
{
}

void ObfuscateTransform::consume(Chunk chunk)
{
    convertToFullIfSparse(chunk);

    obfuscator.train(chunk.getColumns());
    stream_holder->write(getInputPort().getHeader().cloneWithColumns(chunk.getColumns()));

    if (keep_original_data)
        setReadyChunk(std::move(chunk));
}

Chunk ObfuscateTransform::generate()
{
    if (first_generate)
    {
        stream_holder.finishWriting();
        obfuscator.finalize();
        reader.emplace(stream_holder.getReadStream());
        first_generate = false;
    }

    Block block = (*reader)->read();
    if (block.empty())
    {
        obfuscator.updateSeed();
        reader.emplace(stream_holder.getReadStream());
        block = (*reader)->read();

        if (block.empty())
            return {};
    }

    Columns columns = obfuscator.generate(block.getColumns());
    size_t num_rows = block.rows();
    return Chunk(columns, num_rows);
}

}
