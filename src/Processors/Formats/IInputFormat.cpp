#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/WithFileName.h>
#include <Common/StackTrace.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

IInputFormat::IInputFormat(SharedHeader header, ReadBuffer * in_) : ISource(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

Chunk IInputFormat::generate()
{
    LOG_DEBUG(getLogger("IInputFormat"),
          "Generating chunk from input format, possessing buffers: {}, id {}",
          owned_buffers.size(), size_t(this));
    try
    {
        Chunk res = read();
        LOG_DEBUG(getLogger("IInputFormat"),
        "id {} result {}", size_t(this), res.getNumRows());
        return res;
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
    if (in)
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

void IInputFormat::resetOwnedBuffers()
{
    LOG_DEBUG(getLogger("IInputFormat"),
          "Resetting owned buffers, possessing buffers: {}", owned_buffers.size());
    owned_buffers.clear();
}

void IInputFormat::onFinish()
{
    LOG_DEBUG(getLogger("IInputFormat"), "onFinish inst {}", size_t(this));
    resetReadBuffer();
}
}
