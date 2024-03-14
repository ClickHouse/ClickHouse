#include "ReadBufferFromWebServer.h"

#include <Common/logger_useful.h>
#include <IO/ReadWriteBufferFromHTTP.h>
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


ReadBufferFromWebServer::ReadBufferFromWebServer(
    const String & url_,
    ContextPtr context_,
    const ReadSettings & settings_,
    bool use_external_buffer_,
    size_t read_until_position_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0)
    , log(getLogger("ReadBufferFromWebServer"))
    , context(context_)
    , url(url_)
    , buf_size(settings_.remote_fs_buffer_size)
    , read_settings(settings_)
    , use_external_buffer(use_external_buffer_)
    , read_until_position(read_until_position_)
{
}


std::unique_ptr<ReadBuffer> ReadBufferFromWebServer::initialize()
{
    Poco::URI uri(url);
    if (read_until_position)
    {
        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    const auto & settings = context->getSettingsRef();
    const auto & server_settings = context->getServerSettings();

    auto connection_timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, server_settings.keep_alive_timeout);
    connection_timeouts.withConnectionTimeout(std::max<Poco::Timespan>(settings.http_connection_timeout, Poco::Timespan(20, 0)));
    connection_timeouts.withReceiveTimeout(std::max<Poco::Timespan>(settings.http_receive_timeout, Poco::Timespan(20, 0)));

    auto res = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::DISK)
                   .withSettings(read_settings)
                   .withTimeouts(connection_timeouts)
                   .withBufSize(buf_size)
                   .withHostFilter(&context->getRemoteHostFilter())
                   .withExternalBuf(use_external_buffer)
                   .create(credentials);

    if (read_until_position)
        res->setReadUntilPosition(read_until_position);
    if (offset)
        res->seek(offset, SEEK_SET);

    return res;
}


void ReadBufferFromWebServer::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    impl.reset();
}


bool ReadBufferFromWebServer::nextImpl()
{
    if (read_until_position)
    {
        if (read_until_position == offset)
            return false;

        if (read_until_position < offset)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", offset, read_until_position - 1);
    }

    if (!impl)
    {
        impl = initialize();

        if (!use_external_buffer)
        {
            BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
        }
    }

    if (use_external_buffer)
    {
        impl->set(internal_buffer.begin(), internal_buffer.size());
    }
    else
    {
        impl->position() = position();
    }

    chassert(available() == 0);

    chassert(pos >= working_buffer.begin());
    chassert(pos <= working_buffer.end());

    chassert(working_buffer.begin() != nullptr);
    chassert(impl->buffer().begin() != nullptr);

    chassert(impl->available() == 0);

    auto result = impl->next();

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

    chassert(working_buffer.begin() == impl->buffer().begin());

    if (result)
        offset += working_buffer.size();

    return result;
}


off_t ReadBufferFromWebServer::seek(off_t offset_, int whence)
{
    if (impl)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek is allowed only before first read attempt from the buffer");

    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed");

    if (offset_ < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset_);

    offset = offset_;

    return offset;
}


off_t ReadBufferFromWebServer::getPosition()
{
    return offset - available();
}

}
