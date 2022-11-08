#include <Processors/Transforms/ObfuscateTransform.h>

namespace DB
{

ObfuscateTransform::ObfuscateTransform(
    const Block & header_,
    TemporaryDataOnDiskPtr data_,
    const MarkovModelParameters & params_,
    UInt64 seed_,
    bool keep_original_data_)
    : IAccumulatingTransform(header_, header_)
    , obfuscator(header_, seed_, params_)
    , data(std::move(data_))
    , keep_original_data(keep_original_data_)
{
    filestream = &data->createStream(header_, 0, true);
}

void ObfuscateTransform::consume(Chunk chunk)
{
    convertToFullIfSparse(chunk);

    obfuscator.train(chunk.getColumns());
    filestream->write(getInputPort().getHeader().cloneWithColumns(chunk.getColumns()));

    if (keep_original_data)
        setReadyChunk(std::move(chunk));
}

Chunk ObfuscateTransform::generate()
{
    if (first_generate)
    {
        filestream->finishWriting();
        obfuscator.finalize();
        first_generate = false;
    }

    Block block = filestream->read();
    if (!block)
    {
        obfuscator.updateSeed();
        block = filestream->read();

        if (!block)
            return {};
    }

    Columns columns = obfuscator.generate(block.getColumns());
    size_t num_rows = block.rows();
    return Chunk(columns, num_rows);
}


}
