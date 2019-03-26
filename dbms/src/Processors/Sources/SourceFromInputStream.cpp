#include <Processors/Sources/SourceFromInputStream.h>

namespace DB
{

SourceFromInputStream::SourceFromInputStream(Block header, BlockInputStreamPtr stream)
    : ISource(std::move(header)), stream(std::move(stream))
{
}

Chunk SourceFromInputStream::generate()
{
    if (finished)
        return {};

    if (!initialized)
    {
        stream->readPrefix();
        initialized = true;
    }

    auto block = stream->read();
    if (!block)
    {
        stream->readSuffix();
        finished = true;
        return {};
    }

    assertBlocksHaveEqualStructure(getPort().getHeader(), block, "SourceFromInputStream");

    UInt64 num_rows = block.rows();
    return Chunk(block.getColumns(), num_rows);
}

}
