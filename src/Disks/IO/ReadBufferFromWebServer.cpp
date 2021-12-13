#include "ReadBufferFromWebServer.h"

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
    extern const int LOGICAL_ERROR;
}


static constexpr size_t HTTP_MAX_TRIES = 10;
static constexpr size_t WAIT_INIT = 100;

ReadBufferFromWebServer::ReadBufferFromWebServer(
    const String & url_,
    ContextPtr context_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t last_offset_)
    : SeekableReadBuffer(nullptr, 0)
    , log(&Poco::Logger::get("ReadBufferFromWebServer"))
    , context(context_)
    , url(url_)
    , buf_size(settings_.remote_fs_buffer_size)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
    , last_offset(last_offset_)
{
}


std::unique_ptr<ReadBuffer> ReadBufferFromWebServer::initialize()
{
    Poco::URI uri(url);

    ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;

    if (last_offset)
    {
        if (last_offset < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, last_offset - 1);

        headers.emplace_back(std::make_pair("Range", fmt::format("bytes={}-{}", offset, last_offset - 1)));
        LOG_DEBUG(log, "Reading with range: {}-{}", offset, last_offset);
    }
    else
    {
        headers.emplace_back(std::make_pair("Range", fmt::format("bytes={}-", offset)));
        LOG_DEBUG(log, "Reading from offset: {}", offset);
    }

    const auto & settings = context->getSettingsRef();
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
        read_settings,
        headers,
        context->getRemoteHostFilter(),
        use_external_buffer);
}


void ReadBufferFromWebServer::initializeWithRetry()
{
    /// Initialize impl with retry.
    size_t milliseconds_to_wait = WAIT_INIT;
    for (size_t i = 0; i < HTTP_MAX_TRIES; ++i)
    {
        try
        {
            impl = initialize();

            if (use_external_buffer)
            {
                /**
                 * See comment 30 lines lower.
                 */
                impl->set(internal_buffer.begin(), internal_buffer.size());
                assert(working_buffer.begin() != nullptr);
                assert(!internal_buffer.empty());
            }

            break;
        }
        catch (Poco::Exception & e)
        {
            if (i == HTTP_MAX_TRIES - 1)
                throw;

            LOG_ERROR(&Poco::Logger::get("ReadBufferFromWeb"), "Error: {}, code: {}", e.what(), e.code());
            sleepForMilliseconds(milliseconds_to_wait);
            milliseconds_to_wait *= 2;
        }
    }
}


bool ReadBufferFromWebServer::nextImpl()
{
    if (last_offset)
    {
        if (last_offset == offset)
            return false;

        if (last_offset < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, last_offset - 1);
    }

    if (impl)
    {
        if (use_external_buffer)
        {
            /**
            * use_external_buffer -- means we read into the buffer which
            * was passed to us from somewhere else. We do not check whether
            * previously returned buffer was read or not, because this branch
            * means we are prefetching data, each nextImpl() call we can fill
            * a different buffer.
            */
            impl->set(internal_buffer.begin(), internal_buffer.size());
            assert(working_buffer.begin() != nullptr);
            assert(!internal_buffer.empty());
        }
        else
        {
            /**
            * impl was initialized before, pass position() to it to make
            * sure there is no pending data which was not read, because
            * this branch means we read sequentially.
            */
            impl->position() = position();
            assert(!impl->hasPendingData());
        }
    }
    else
    {
        initializeWithRetry();
    }

    auto result = impl->next();
    if (result)
    {
        BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
        offset += working_buffer.size();
    }

    return result;
}


off_t ReadBufferFromWebServer::seek(off_t offset_, int whence)
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


off_t ReadBufferFromWebServer::getPosition()
{
    return offset - available();
}

}
