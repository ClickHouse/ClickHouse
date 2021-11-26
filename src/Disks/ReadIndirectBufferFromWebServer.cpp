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

    ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;
    headers.emplace_back(std::make_pair("Range", fmt::format("bytes={}-", offset)));
    LOG_DEBUG(log, "Reading from offset: {}", offset);

    return std::make_unique<ReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        ReadWriteBufferFromHTTP::OutStreamCallback(),
        ConnectionTimeouts::getHTTPTimeouts(context),
        0,
        Poco::Net::HTTPBasicCredentials{},
        buf_size,
        headers);
}


bool ReadIndirectBufferFromWebServer::nextImpl()
{
    bool next_result = false, successful_read = false;

    if (impl)
    {
        /// Restore correct position at the needed offset.
        impl->position() = position();
        assert(!impl->hasPendingData());
    }
    else
    {
        try
        {
            impl = initialize();
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Unreachable url: {}. Error: {}", url, e.what());
        }

        next_result = impl->hasPendingData();
    }

    for (size_t try_num = 0; (try_num < max_read_tries) && !next_result; ++try_num)
    {
        try
        {
            next_result = impl->next();
            successful_read = true;
            break;
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Read attempt {}/{} failed from {}. ({})", try_num, max_read_tries, url, e.message());

            impl.reset();
            impl = initialize();
            next_result = impl->hasPendingData();
        }
    }

    if (!successful_read)
        throw Exception(ErrorCodes::NETWORK_ERROR, "All read attempts ({}) failed for uri: {}", max_read_tries, url);

    if (next_result)
    {
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
        offset += working_buffer.size();
    }

    return next_result;
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
    return offset - available();
}

}
