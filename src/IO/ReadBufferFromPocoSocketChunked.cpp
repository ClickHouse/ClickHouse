#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <Common/logger_useful.h>
#include <IO/NetUtils.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

ReadBufferFromPocoSocketChunked::ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size)
    : ReadBufferFromPocoSocketChunked(socket_, ProfileEvents::end(), buf_size)
{}

ReadBufferFromPocoSocketChunked::ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size)
    : ReadBuffer(nullptr, 0), log(getLogger("Protocol")), buffer_socket(socket_, read_event_, buf_size)
{
    chassert(buf_size <= std::numeric_limits<decltype(chunk_left)>::max());

    working_buffer = buffer_socket.buffer();
    pos = buffer_socket.position();
}

void ReadBufferFromPocoSocketChunked::enableChunked()
{
    chunked = true;
    buffer_socket.position() = pos;
}

bool ReadBufferFromPocoSocketChunked::poll(size_t timeout_microseconds)
{
    if (!chunked)
        buffer_socket.position() = pos;

    return buffer_socket.poll(timeout_microseconds);
}

void ReadBufferFromPocoSocketChunked::setAsyncCallback(AsyncCallback async_callback_)
{
    buffer_socket.setAsyncCallback(async_callback_);
}

bool ReadBufferFromPocoSocketChunked::startChunk()
{
    if (buffer_socket.read(reinterpret_cast<char *>(&chunk_left), sizeof(chunk_left)) < sizeof(chunk_left))
        return false;
    if (chunk_left == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Native protocol: empty chunk received");

    chunk_left = fromLittleEndian(chunk_left);

    return nextChunk();
}

bool ReadBufferFromPocoSocketChunked::nextChunk()
{
    if (chunk_left == 0)
    {
        started = true;
        return startChunk();
    }

    if (buffer_socket.available() == 0)
        if (!buffer_socket.next())
            return false;
    if (started)
        LOG_TEST(log, "Packet receive started. Message {}, size {}", static_cast<unsigned int>(*buffer_socket.position()), chunk_left);
    else
        LOG_TEST(log, "Packet receive continued. Size {}", chunk_left);

    started = false;

    nextimpl_working_buffer_offset = buffer_socket.offset();

    if (buffer_socket.available() < chunk_left)
    {
        working_buffer.resize(buffer_socket.offset() + buffer_socket.available());
        chunk_left -= buffer_socket.available();
        buffer_socket.position() += buffer_socket.available();
        return true;
    }

    working_buffer.resize(buffer_socket.offset() + chunk_left);
    UInt8 buffered = std::min(static_cast<size_t>(4), buffer_socket.available() - chunk_left);

    buffer_socket.position() += chunk_left;
    if (buffered > 0)
        std::memcpy(&chunk_left, buffer_socket.position(), buffered);
    buffer_socket.position() += buffered;

    if (4 > buffered)
        if (!buffer_socket.readSocketExact(reinterpret_cast<Position>(&chunk_left) + buffered, 4 - buffered))
            return false;

    chunk_left = fromLittleEndian(chunk_left);

    if (chunk_left == 0)
        LOG_TEST(log, "Packet receive ended.");

    return true;
}


bool ReadBufferFromPocoSocketChunked::nextImpl()
{
    if (chunked)
    {
        if (!nextChunk())
        {
            pos = buffer_socket.position();
            return false;
        }
        return true;
    }

    buffer_socket.position() = pos;

    if (!buffer_socket.next())
    {
        pos = buffer_socket.position();
        return false;
    }

    pos = buffer_socket.position();
    working_buffer.resize(offset() + buffer_socket.available());

    return true;
}

}
