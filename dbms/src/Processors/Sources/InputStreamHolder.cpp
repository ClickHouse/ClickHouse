#include <Processors/Sources/InputStreamHolder.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

Block InputStreamHolder::read()
{
    if (stream_finished)
        return {};

    if (!initialized)
    {
        stream->readPrefix();
        initialized = true;
    }

    return stream->read();
}

void InputStreamHolder::readSuffix()
{
    std::lock_guard lock_guard(lock);

    if (stream_finished)
        return;

    if (!initialized)
    {
        stream->readPrefix();
        initialized = true;
    }

    stream->readSuffix();
    stream_finished = true;
}

}
