#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <Common/logger_useful.h>
#include <IO/NetUtils.h>

#include <cstring>


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
    : ReadBufferFromPocoSocketBase(
        socket_, read_event_,
        std::min(buf_size, static_cast<size_t>(std::numeric_limits<decltype(chunk_left)>::max()))),
        our_address(socket_.address()), log(getLogger("Protocol"))
{}

void ReadBufferFromPocoSocketChunked::enableChunked()
{
    if (chunked)
        return;
    chunked = 1;
    data_end = buffer().end();
    /// Resize working buffer so any next read will call nextImpl
    working_buffer.resize(offset());
    chunk_left = 0;
    next_chunk = 0;
}

bool ReadBufferFromPocoSocketChunked::hasBufferedChunkData() const
{
    if (!chunked)
        return false;

    const auto buffered = static_cast<size_t>(data_end - working_buffer.end());
    /// In the middle of a chunk, the buffered bytes (left by `appendWithoutWaiting`) are its data. Otherwise they
    /// start with the end marker of the chunk or the size of its next part, which is already read into `next_chunk`.
    if (chunk_left)
        return buffered > 0;
    return buffered > sizeof(next_chunk);
}

bool ReadBufferFromPocoSocketChunked::hasBufferedData() const
{
    return available() || hasBufferedChunkData();
}

bool ReadBufferFromPocoSocketChunked::appendWithoutWaiting(size_t max_bytes, bool read_socket)
{
    if (chunked)
        return appendChunkedWithoutWaiting(max_bytes, read_socket);

    if (!read_socket || max_bytes == 0)
        return true;

    const size_t unread = available();
    if (pos != internal_buffer.begin())
    {
        bytes += offset();
        std::memmove(internal_buffer.begin(), pos, unread);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + unread);
        pos = internal_buffer.begin();
    }

    max_bytes = std::min(max_bytes, internal_buffer.size() - unread);
    if (max_bytes == 0)
        return true;

    applyHandshakeDeadlineToSocket();
    const ssize_t bytes_read = socketReceiveBytesImpl(internal_buffer.begin() + unread, max_bytes);
    if (read_event != ProfileEvents::end())
        ProfileEvents::increment(read_event, bytes_read);

    working_buffer.resize(unread + bytes_read);
    return bytes_read > 0;
}

bool ReadBufferFromPocoSocketChunked::appendChunkedWithoutWaiting(size_t max_bytes, bool read_socket)
{
    constexpr size_t frame_size = sizeof(next_chunk);
    char * const begin = internal_buffer.begin();
    const size_t unread = available();

    /// The state of the stream after the working buffer, see `nextImpl`. `raw_offset` is where the received bytes
    /// that are not consumed yet start, relative to the end of the working buffer.
    size_t raw_offset = 0;
    size_t payload_left = 0;     /// The bytes of the current chunk (part) that are not in the working buffer.
    bool need_header = false;    /// The current chunk has ended, the next one starts with its size.
    if (chunked == 1)
        need_header = true; /// The first chunk, it is not preceded by an end marker.
    else if (chunk_left)
        payload_left = chunk_left;
    else
    {
        /// The end marker of the chunk or the size of its next part is already read into `next_chunk`.
        raw_offset = frame_size;
        if (next_chunk)
            payload_left = next_chunk;
        else
            need_header = true;
    }

    const auto buffered_after = static_cast<size_t>(data_end - working_buffer.end());
    const size_t raw_size = buffered_after > raw_offset ? buffered_after - raw_offset : 0;

    /// Move the unread bytes and the received ones after them to the beginning of the buffer.
    bytes += offset();
    std::memmove(begin, pos, unread);
    if (raw_size)
        std::memmove(begin + unread, working_buffer.end() + raw_offset, raw_size);

    char * out = begin + unread;    /// Where the next bytes of the stream go.
    char * raw_pos = out;           /// The received bytes that are not consumed yet.
    char * raw_end = out + raw_size;

    bool end_of_stream = false;
    if (read_socket)
    {
        /// A bounded read: enough for `max_bytes` with the framing of a few chunks around them. Leave room for the
        /// end marker placed before the beginning of the next chunk (see below). If the buffer is full of the received
        /// bytes, take only them.
        const size_t room = static_cast<size_t>(internal_buffer.end() - raw_end);
        const size_t to_read = std::min(max_bytes + 4 * frame_size, room > frame_size ? room - frame_size : 0);
        if (to_read)
        {
            applyHandshakeDeadlineToSocket();
            const ssize_t bytes_read = socketReceiveBytesImpl(raw_end, to_read);
            if (read_event != ProfileEvents::end())
                ProfileEvents::increment(read_event, bytes_read);
            end_of_stream = bytes_read == 0;
            raw_end += bytes_read;
        }
    }

    /// Remove the framing from the received bytes, as `nextImpl` would do, but stop where it would wait.
    size_t appended = 0;
    bool marker_pending = false;    /// The current chunk (part) is complete, the next 4 bytes are its end marker or the size of its next part.
    while (true)
    {
        const auto raw_left = static_cast<size_t>(raw_end - raw_pos);
        if (need_header || marker_pending)
        {
            if (raw_left < frame_size)
                break;
            UInt32 value = 0;
            std::memcpy(&value, raw_pos, frame_size);
            value = fromLittleEndian(value);
            raw_pos += frame_size;

            if (need_header)
            {
                if (value == 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Native protocol: empty chunk received");
                payload_left = value;
                need_header = false;
                chunked = 2;
            }
            else
            {
                marker_pending = false;
                if (value)
                    payload_left = value;
                else
                    need_header = true;
            }
            continue;
        }

        const size_t n = std::min({payload_left, raw_left, max_bytes - appended});
        if (n == 0)
            break;
        std::memmove(out, raw_pos, n);
        out += n;
        raw_pos += n;
        appended += n;
        payload_left -= n;
        if (payload_left == 0)
            marker_pending = true;
    }

    /// Bring the state to one that `nextImpl` continues from.
    const auto raw_left = static_cast<size_t>(raw_end - raw_pos);
    next_chunk = 0;
    if (marker_pending)
    {
        /// There is no state for a complete chunk whose end marker is not read yet, so leave the last byte of the
        /// chunk (it was appended by this call) after the working buffer: `nextImpl` reads it and then the marker.
        chassert(appended > 0);
        --out;
        std::memmove(out + 1, raw_pos, raw_left);
        chunk_left = 1;
        data_end = out + 1 + raw_left;
    }
    else if (need_header)
    {
        chunk_left = 0;
        if (chunked == 1 || raw_left == 0)
        {
            std::memmove(out, raw_pos, raw_left);
            data_end = out + raw_left;
        }
        else
        {
            /// As after `loadNextChunk` has read the end marker: `nextImpl` skips 4 bytes after the working buffer.
            if (static_cast<size_t>(internal_buffer.end() - out) < frame_size + raw_left)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No room in the buffer for the beginning of the next chunk");
            char header[frame_size];
            std::memcpy(header, raw_pos, raw_left);
            std::memset(out, 0, frame_size);
            std::memcpy(out + frame_size, header, raw_left);
            data_end = out + frame_size + raw_left;
        }
    }
    else
    {
        /// In the middle of a chunk (part): the buffered bytes after the working buffer are its data.
        chassert(payload_left > 0);
        std::memmove(out, raw_pos, raw_left);
        chunk_left = static_cast<UInt32>(payload_left);
        data_end = out + raw_left;
    }

    working_buffer = Buffer(begin, out);
    pos = begin;
    return !end_of_stream;
}

bool ReadBufferFromPocoSocketChunked::poll(size_t timeout_microseconds)
{
    if (available() || hasBufferedChunkData())
        return true;

    return ReadBufferFromPocoSocketBase::poll(timeout_microseconds);
}


bool ReadBufferFromPocoSocketChunked::loadNextChunk(Position c_pos, bool cont)
{
    auto buffered = std::min(static_cast<size_t>(data_end - c_pos), sizeof(next_chunk));

    if (buffered)
        std::memcpy(&next_chunk, c_pos, buffered);
    if (buffered < sizeof(next_chunk))
        if (socketReceiveBytesImpl(reinterpret_cast<char *>(&next_chunk) + buffered, sizeof(next_chunk) - buffered) < static_cast<ssize_t>(sizeof(next_chunk) - buffered))
            return false;
    next_chunk = fromLittleEndian(next_chunk);

    if (next_chunk)
    {
        if (cont)
            LOG_TEST(log, "{} <- {} Chunk receive continued. Size {}", ourAddress().toString(), peerAddress().toString(), next_chunk);
    }
    else
        LOG_TEST(log, "{} <- {} Chunk receive ended.", ourAddress().toString(), peerAddress().toString());

    return true;
}

bool ReadBufferFromPocoSocketChunked::processChunkLeft(Position c_pos)
{
    if (data_end - c_pos < chunk_left)
    {
        working_buffer.resize(data_end - buffer().begin());
        nextimpl_working_buffer_offset = c_pos - buffer().begin();
        chunk_left -= (data_end - c_pos);
        return true;
    }

    nextimpl_working_buffer_offset = c_pos - buffer().begin();
    working_buffer.resize(nextimpl_working_buffer_offset + chunk_left);

    c_pos += chunk_left;

    if (!loadNextChunk(c_pos, true))
        return false;

    chunk_left = 0;
    return true;
}


bool ReadBufferFromPocoSocketChunked::nextImpl()
{
    if (!chunked)
        return ReadBufferFromPocoSocketBase::nextImpl();

    auto * c_pos = pos;

    if (chunk_left == 0)
    {
        if (next_chunk == 0)
        {
            if (chunked == 1)
                chunked = 2; // first chunked block - no end marker
            else
                c_pos = pos + sizeof(next_chunk); // bypass chunk end marker

            c_pos = std::min(c_pos, data_end);

            if (!loadNextChunk(c_pos))
                return false;

            chunk_left = next_chunk;
            next_chunk = 0;

            if (chunk_left == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Native protocol: empty chunk received");

            c_pos += sizeof(next_chunk);

            if (c_pos >= data_end)
            {
                if (!ReadBufferFromPocoSocketBase::nextImpl())
                    return false;
                data_end = buffer().end();
                c_pos = buffer().begin();
            }

            LOG_TEST(log, "{} <- {} Chunk receive started. Message {}, size {}", ourAddress().toString(), peerAddress().toString(), static_cast<unsigned int>(*c_pos), chunk_left);
        }
        else
        {
            c_pos += sizeof(next_chunk);
            if (c_pos >= data_end)
            {
                if (!ReadBufferFromPocoSocketBase::nextImpl())
                    return false;
                data_end = buffer().end();
                c_pos = buffer().begin();
            }

            chunk_left = next_chunk;
            next_chunk = 0;
        }
    }
    else if (data_end > working_buffer.end())
    {
        /// The data of the chunk buffered by `appendWithoutWaiting`.
        c_pos = working_buffer.end();
    }
    else
    {
        if (!ReadBufferFromPocoSocketBase::nextImpl())
            return false;
        data_end = buffer().end();
        c_pos = buffer().begin();
    }

    return processChunkLeft(c_pos);
}

}
