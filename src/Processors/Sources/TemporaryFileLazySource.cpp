#include <Processors/Sources/TemporaryFileLazySource.h>
#include <Formats/TemporaryFileStream.h>

namespace DB
{

TemporaryFileLazySource::~TemporaryFileLazySource() = default;

TemporaryFileLazySource::TemporaryFileLazySource(const std::string & path_, const Block & header_)
    : ISource(header_)
    , path(path_)
    , done(false)
{}

Chunk TemporaryFileLazySource::generate()
{
    if (done)
        return {};

    if (!stream)
        stream = std::make_unique<TemporaryFileStream>(path, header);

    auto block = stream->block_in->read();
    if (!block)
    {
        done = true;
        stream.reset();
    }
    return Chunk(block.getColumns(), block.rows());
}

}
