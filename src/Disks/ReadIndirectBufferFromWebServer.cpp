#include "ReadIndirectBufferFromWebServer.h"

#include <base/logger_useful.h>
#include <base/sleep.h>
#include <Core/Types.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int NETWORK_ERROR;
}

static const auto WAIT_MS = 10;


ReadIndirectBufferFromWebServer::ReadIndirectBufferFromWebServer(
    const String & url_, ContextPtr context_, size_t buf_size_, size_t backoff_threshold_, size_t max_tries_)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
    , log(&Poco::Logger::get("ReadIndirectBufferFromWebServer"))
    , context(context_)
    , url(url_)
    , buf_size(buf_size_)
    , backoff_threshold_ms(backoff_threshold_)
    , max_tries(max_tries_)
{
}


std::unique_ptr<ReadBuffer> ReadIndirectBufferFromWebServer::initialize()
{
    Poco::URI uri(url);

    ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;
    headers.emplace_back(std::make_pair("Range", fmt::format("bytes={}-", offset)));
    const auto & settings = context->getSettingsRef();
    LOG_DEBUG(log, "Reading from offset: {}", offset);
    const auto & config = context->getConfigRef();
    Poco::Timespan http_keep_alive_timeout{config.getUInt("keep_alive_timeout", 20), 0};

    return std::make_unique<ReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_GET,
        ReadWriteBufferFromHTTP::OutStreamCallback(),
        ConnectionTimeouts(std::max(Poco::Timespan(settings.http_connection_timeout.totalSeconds(), 0), Poco::Timespan(20, 0)),
                           settings.http_send_timeout,
                           std::max(Poco::Timespan(settings.http_receive_timeout.totalSeconds(), 0), Poco::Timespan(20, 0)),
                           settings.tcp_keep_alive_timeout,
                           http_keep_alive_timeout),
        0,
        Poco::Net::HTTPBasicCredentials{},
        buf_size,
        headers);
}


bool ReadIndirectBufferFromWebServer::nextImpl()
{
    bool next_result = false, successful_read = false;
    UInt16 milliseconds_to_wait = WAIT_MS;

    if (impl)
    {
        /// Restore correct position at the needed offset.
        impl->position() = position();
        assert(!impl->hasPendingData());
    }

    WriteBufferFromOwnString error_msg;
    for (size_t i = 0; (i < max_tries) && !successful_read && !next_result; ++i)
    {
        while (milliseconds_to_wait < backoff_threshold_ms)
        {
            try
            {
                if (!impl)
                {
                    impl = initialize();
                    next_result = impl->hasPendingData();
                    if (next_result)
                        break;
                }

                next_result = impl->next();
                successful_read = true;
                break;
            }
            catch (const Poco::Exception & e)
            {
                LOG_WARNING(log, "Read attempt failed for url: {}. Error: {}", url, e.what());
                error_msg << fmt::format("Error: {}\n", e.what());

                sleepForMilliseconds(milliseconds_to_wait);
                milliseconds_to_wait *= 2;
                impl.reset();
            }
        }
        milliseconds_to_wait = WAIT_MS;
    }

    if (!successful_read)
        throw Exception(ErrorCodes::NETWORK_ERROR,
                        "All read attempts failed for url: {}. Reason:\n{}", url, error_msg.str());

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
