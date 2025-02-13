#pragma once 

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "IO/WriteBufferFromString.h"
#include "base/types.h"

#include <bson/bson.h>
#include "Common/Exception.h"

#include <iostream>
#include <stdexcept>

namespace DB
{

namespace MongoProtocol
{

enum class OperationCode : Int32
{
    OP_MSG = 2013,
    OP_REPLY = 1,
    OP_UPDATE = 2001,
    OP_INSERT = 2002,
    RESERVED = 2003,
    OP_QUERY = 2004,
    OP_GET_MORE = 2005,
    OP_DELETE = 2006,
    OP_KILL_CURSORS = 2007,
    OP_COMPRESSED = 2012
};

class MessageTransport
{
private:
    ReadBuffer * in;
    WriteBuffer * out;

public:
    explicit MessageTransport(WriteBuffer * out_) : in(nullptr), out(out_) {}

    MessageTransport(ReadBuffer * in_, WriteBuffer * out_): in(in_), out(out_) {}

    template<typename TMessage>
    std::unique_ptr<TMessage> receiveWithPayloadSize(Int32 payload_size)
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>(payload_size);
        message->deserialize(*in);
        return message;
    }

    template<typename TMessage>
    std::unique_ptr<TMessage> receive()
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>();
        message->deserialize(*in);
        return message;
    }

    template<typename TMessage>
    void send(TMessage & message, bool flush=false)
    {
        message.serialize(*out);
        if (flush)
            out->next();
    }

    template<typename TMessage>
    void send(TMessage && message, bool flush=false)
    {
        send(message, flush);
    }

    void send(char message, bool flush=false)
    {
        out->write(message);
        if (flush)
            out->next();
    }

    void dropMessage()
    {
        Int32 size;
        readBinaryBigEndian(size, *in);
        in->ignore(size - 4);
    }

    void flush()
    {
        out->next();
    }
};

/** Basic class for messages sent by client or server. */
class IMessage
{
public:
    virtual ~IMessage() = default;
};

class ISerializable
{
public:
    /** Should be overridden for sending the message */
    virtual void serialize(WriteBuffer & out) const = 0;

    /** Size of the message in bytes including message length part (4 bytes) */
    virtual Int32 size() const = 0;

    ISerializable() = default;

    ISerializable(const ISerializable &) = default;

    virtual ~ISerializable() = default;
};

class FrontMessage : public IMessage
{
public:
    /** Should be overridden for receiving the message
     * NB: This method should not read the first byte, which means the type of the message
     * (if type is provided for the message by the protocol).
     */
    virtual void deserialize(ReadBuffer & in) = 0;
};

class BackendMessage : public IMessage, public ISerializable
{};

struct Header : public FrontMessage
{
    Int32 message_length;
    Int32 request_id;
    Int32 response_to;
    Int32 operation_code;

    void deserialize(ReadBuffer & in) override
    {
        readBinaryLittleEndian(message_length, in);
        readBinaryLittleEndian(request_id, in);
        readBinaryLittleEndian(response_to, in);
        readBinaryLittleEndian(operation_code, in);
    }
};

struct OperationQuery : public FrontMessage
{
    Int32 flags;
    String full_collection_name;
    Int32 number_to_skip;
    Int32 number_to_return;
    
    Int32 size_query;
    String query;
    Int32 size_projection;
    String projection;

    void deserialize(ReadBuffer & in) override
    {
        readBinaryLittleEndian(flags, in);
        readNullTerminated(full_collection_name, in);
        readBinaryLittleEndian(number_to_skip, in);
        readBinaryLittleEndian(number_to_skip, in);

        readBinaryLittleEndian(size_query, in);
        {
            String bson_size;
            {
                WriteBufferFromString buf_size(bson_size);
                writeBinaryLittleEndian(size_query, buf_size);
            }
            std::cerr << "size_query " << size_query << '\n';
            query.resize(size_query - 4);
            auto readed = static_cast<Int32>(in.read(query.data(), size_query - 4));
            if (readed != size_query - 4) {
                std::cerr << "size_query excp " << size_query << ' ' << readed << '\n';
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Can not read query message from transport");
            }
            query = bson_size + query;
            std::cerr << "result query " << query << '\n';
        }

        if (!in.eof())
        {
            std::cerr << "not eof\n";
            readBinaryLittleEndian(size_projection, in);
            std::cerr << "size_projection " << size_projection << '\n';
            {
                projection.resize(size_projection);
                if (static_cast<Int32>(in.read(projection.data(), size_projection)) != size_projection) {
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Can not read query message from transport");
                }
            }
        }
    }
};

}

}
