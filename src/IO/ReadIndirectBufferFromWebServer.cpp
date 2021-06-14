#include "ReadIndirectBufferFromWebServer.h"

#include <common/logger_useful.h>
#include <Core/Types.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int NETWORK_ERROR;
}


ReadIndirectBufferFromWebServer::ReadIndirectBufferFromWebServer(const String & url_,
                                                                 ContextPtr context_,
                                                                 size_t max_read_tries_,
                                                                 size_t buf_size_)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
    , log(&Poco::Logger::get("ReadIndirectBufferFromWebServer"))
    , context(context_)
    , url(url_)
    , buf_size(buf_size_)
    , max_read_tries(max_read_tries_)
{
}


std::unique_ptr<ReadBuffer> ReadIndirectBufferFromWebServer::initialize()
{
    Poco::URI uri(url);
    return std::make_unique<ReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        ReadWriteBufferFromHTTP::OutStreamCallback(),
        ConnectionTimeouts::getHTTPTimeouts(context),
        0,
        Poco::Net::HTTPBasicCredentials{},
        buf_size);
}


bool ReadIndirectBufferFromWebServer::nextImpl()
{
    if (!impl)
        impl = initialize();

    pos = impl->position();

    bool ret = false, successful_read = false;
    auto sleep_milliseconds = std::chrono::milliseconds(100);

    for (size_t try_num = 0; try_num < max_read_tries; ++try_num)
    {
        try
        {
            ret = impl->next();
            successful_read = true;
            break;
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Read attempt {}/{} failed from {}. ({})", try_num, max_read_tries, url, e.message());
        }

        std::this_thread::sleep_for(sleep_milliseconds);
        sleep_milliseconds *= 2;
    }

    if (!successful_read)
        throw Exception(ErrorCodes::NETWORK_ERROR, "All read attempts ({}) failed for url {}", max_read_tries, url);

    if (ret)
    {
        working_buffer = internal_buffer = impl->buffer();
        /// Do not update pos here, because it is anyway overwritten after nextImpl() in ReadBuffer::next().
    }

    return ret;
}


off_t ReadIndirectBufferFromWebServer::seek(off_t offset_, int whence)
{
    if (impl)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek is allowed only before first read attempt from the buffer");

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", std::to_string(offset_));

    offset = offset_;

    return offset;
}


off_t ReadIndirectBufferFromWebServer::getPosition()
{
    return offset + count();
}

}
