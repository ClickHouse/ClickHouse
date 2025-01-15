#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/WithFileName.h>
#include <Common/Exception.h>

namespace DB
{

IInputFormat::IInputFormat(Block header, ReadBuffer * in_)
    : SourceWithKeyCondition(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

Chunk IInputFormat::generate()
{
    try
    {
        size_t total_rows_before = total_rows;
        auto chunk = read();
        if (!chunk.getChunkInfos().get<ChunkInfoReadRowsBefore>())
            chunk.getChunkInfos().add(std::make_shared<ChunkInfoReadRowsBefore>(total_rows_before));
        return chunk;
    }
    catch (Exception & e)
    {
        auto file_name = getFileNameFromReadBuffer(getReadBuffer());
        if (!file_name.empty())
            e.addMessage(fmt::format("(in file/uri {})", file_name));
        throw;
    }
}

void IInputFormat::resetParser()
{
    chassert(in);
    in->ignoreAll();
    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = &in_;
}

Chunk IInputFormat::getChunkForCount(size_t rows)
{
    const auto & header = getPort().getHeader();
    return cloneConstWithDefault(Chunk{header.getColumns(), 0}, rows);
}

}
