#pragma once

#include <Common/logger_useful.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/NetUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class WriteBufferFromPocoSocketChunked: public WriteBufferFromPocoSocket
{
public:
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : WriteBufferFromPocoSocket(socket_, buf_size), log(getLogger("Protocol")) {}
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : WriteBufferFromPocoSocket(socket_, write_event_, buf_size), log(getLogger("Protocol")) {}

    void enableChunked()
    {
        chunked = true;
        /// Initialize next chunk
        chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(pos);
        pos += std::min(available(), sizeof(*chunk_size_ptr));
    }

    void finishChunk()
    {
        if (!chunked)
            return;

        if (pos <= reinterpret_cast<Position>(chunk_size_ptr) + sizeof(*chunk_size_ptr))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Native protocol: attempt to send empty chunk");

        /// Fill up current chunk size
        *chunk_size_ptr = toLittleEndian(static_cast<UInt32>(pos - reinterpret_cast<Position>(chunk_size_ptr) - sizeof(*chunk_size_ptr)));

        if (!chunk_started)
            LOG_TEST(log, "{} -> {} Chunk send started. Message {}, size {}",
                    ourAddress().toString(), peerAddress().toString(),
                    static_cast<unsigned int>(*(reinterpret_cast<char *>(chunk_size_ptr) + sizeof(*chunk_size_ptr))),
                    *chunk_size_ptr);
        else
            chunk_started = false;

        LOG_TEST(log, "{} -> {} Chunk send ended.", ourAddress().toString(), peerAddress().toString());

        if (available() < sizeof(*chunk_size_ptr))
        {
            finishing = available();
            pos += available();
            chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(pos);
            return;
        }

        /// Buffer end-of-chunk
        *reinterpret_cast<decltype(chunk_size_ptr)>(pos) = 0;
        pos += sizeof(*chunk_size_ptr);
        /// Initialize next chunk
        chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(pos);
        pos += std::min(available(), sizeof(*chunk_size_ptr));
    }

protected:
    void nextImpl() override
    {
        if (!chunked)
            return WriteBufferFromPocoSocket::nextImpl();

        if (finishing < sizeof(*chunk_size_ptr))
        {
            pos -= finishing;
            /// Send current chunk
            WriteBufferFromPocoSocket::nextImpl();
            /// Send end-of-chunk directly
            UInt32 s = 0;
            socketSendBytes(reinterpret_cast<const char *>(&s), sizeof(s));

            finishing = sizeof(*chunk_size_ptr);

            /// Initialize next chunk
            chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(working_buffer.begin());
            nextimpl_working_buffer_offset = sizeof(*chunk_size_ptr);

            return;
        }

        if (offset() == sizeof(*chunk_size_ptr)) // prevent sending empty chunk
        {
            nextimpl_working_buffer_offset = sizeof(*chunk_size_ptr);
            return;
        }

        if (working_buffer.end() - reinterpret_cast<Position>(chunk_size_ptr) <= static_cast<std::ptrdiff_t>(sizeof(*chunk_size_ptr)))
        {
            pos = reinterpret_cast<Position>(chunk_size_ptr);
            /// Send current chunk
            WriteBufferFromPocoSocket::nextImpl();
            /// Initialize next chunk
            chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(working_buffer.begin());
            nextimpl_working_buffer_offset = sizeof(*chunk_size_ptr);

            return;
        }

        if (pos - reinterpret_cast<Position>(chunk_size_ptr) == sizeof(*chunk_size_ptr))
            pos -= sizeof(*chunk_size_ptr);
        else /// Fill up current chunk size
        {
            *chunk_size_ptr = toLittleEndian(static_cast<UInt32>(pos - reinterpret_cast<Position>(chunk_size_ptr) - sizeof(*chunk_size_ptr)));
            if (!chunk_started)
            {
                chunk_started = true;
                LOG_TEST(log, "{} -> {} Chunk send started. Message {}, size {}",
                        ourAddress().toString(), peerAddress().toString(),
                        static_cast<unsigned int>(*(reinterpret_cast<char *>(chunk_size_ptr) + sizeof(*chunk_size_ptr))),
                        *chunk_size_ptr);
            }
            else
                LOG_TEST(log, "{} -> {} Chunk send continued. Size {}", ourAddress().toString(), peerAddress().toString(), *chunk_size_ptr);
        }
        /// Send current chunk
        WriteBufferFromPocoSocket::nextImpl();
        /// Initialize next chunk
        chunk_size_ptr = reinterpret_cast<decltype(chunk_size_ptr)>(working_buffer.begin());
        nextimpl_working_buffer_offset = sizeof(*chunk_size_ptr);
    }

    Poco::Net::SocketAddress peerAddress()
    {
        return peer_address;
    }

    Poco::Net::SocketAddress ourAddress()
    {
        return our_address;
    }
private:
    LoggerPtr log;
    bool chunked = false;
    bool chunk_started = false; // chunk started flag
    UInt32 * chunk_size_ptr = nullptr; // pointer to the chunk size holder in the buffer 
    size_t finishing = sizeof(*chunk_size_ptr); // indicates not enough buffer for end-of-chunk marker
};

}
