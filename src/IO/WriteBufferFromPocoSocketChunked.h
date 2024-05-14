#pragma once

#include <Common/logger_useful.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/NetUtils.h>


namespace DB
{

class WriteBufferFromPocoSocketChunked: public WriteBufferFromPocoSocket
{
public:
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : WriteBufferFromPocoSocket(socket_, buf_size), log(getLogger("Protocol")) {}
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE) : WriteBufferFromPocoSocket(socket_, write_event_, buf_size), log(getLogger("Protocol")) {}

    void enableChunked() { chunked = true; }
    void finishPacket()
    {
        if (!chunked)
            return;

        next();

        if (finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Native protocol: attempt to send empty chunk");

        LOG_TEST(log, "Packet send ended.");
        finished = true;

        UInt32 s = 0;
        socketSendBytes(reinterpret_cast<const char *>(&s), sizeof(s));
    }
protected:
    void nextImpl() override
    {
        if (chunked)
        {
            UInt32 s = static_cast<UInt32>(offset());
            if (finished)
                LOG_TEST(log, "Packet send started. Message {}, size {}", static_cast<unsigned int>(*buffer().begin()), s);
            else
                LOG_TEST(log, "Packet send continued. Size {}", s);

            finished = false;
            s = hostToNet(s);
            socketSendBytes(reinterpret_cast<const char *>(&s), sizeof(s));
        }

        WriteBufferFromPocoSocket::nextImpl();
    }
private:
    LoggerPtr log;
    bool chunked = false;
    bool finished = true;
};

}
