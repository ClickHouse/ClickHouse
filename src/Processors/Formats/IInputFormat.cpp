#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>


namespace DB
{

IInputFormat::IInputFormat(Block header, ReadBuffer * in_)
    : ISource(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
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
