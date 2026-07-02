#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "Interpreters/Session.h"
#include "base/types.h"

#include <bson/bson.h>

#include <Poco/Net/SocketAddress.h>

#include <pcg_random.hpp>
#include <Common/randomSeed.h>

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

/** Basic class for messages sent by client or server. */

class ISerializable
{
public:
    /** Should be overridden for sending the message */
    virtual void serialize(WriteBuffer & out) const = 0;

    /** Size of the message in bytes including message length part (4 bytes) */
    virtual Int32 size() const = 0;

    virtual ~ISerializable() = default;
};

class FrontMessage
{
public:
    /** Should be overridden for receiving the message
     * NB: This method should not read the first byte, which means the type of the message
     * (if type is provided for the message by the protocol).
     */
    virtual void deserialize(ReadBuffer & in) = 0;

    virtual ~FrontMessage() = default;
};

class BackendMessage : public ISerializable
{
};

struct Header : public FrontMessage, BackendMessage
{
    UInt32 message_length;
    UInt32 request_id;
    UInt32 response_to;
    UInt32 operation_code;

    Header() = default;
    Header(const Header & other);

    Header & operator=(const Header & right);

    void deserialize(ReadBuffer & in) override;

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override;
};

class MessageTransport
{
private:
    ReadBuffer * in;
    WriteBuffer * out;
    Int32 response_counter = 0;

public:
    explicit MessageTransport(WriteBuffer * out_) : in(nullptr), out(out_) { }

    MessageTransport(ReadBuffer * in_, WriteBuffer * out_) : in(in_), out(out_) { }

    template <typename TMessage>
    std::unique_ptr<TMessage> receiveWithPayloadSize(Int32 payload_size)
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>(payload_size);
        message->deserialize(*in);
        return message;
    }

    template <typename TMessage>
    std::unique_ptr<TMessage> receive()
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>();
        message->deserialize(*in);
        return message;
    }

    template <typename TMessage>
    void send(TMessage & message, bool flush = false)
    {
        message.serialize(*out);
        if (flush)
            out->next();
    }

    template <typename TMessage>
    void send(TMessage && message, bool flush = false)
    {
        send(message, flush);
    }

    void send(char message, bool flush = false)
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

    void flush() { out->next(); }

    Int32 getNextResponseId() { return ++response_counter; }
};

class QueryExecutor
{
public:
    explicit QueryExecutor(std::unique_ptr<Session> & session_, const Poco::Net::SocketAddress & address_);

    String execute(const String & query);

    void authenticate(const String & username, const String & password);

private:
    std::unique_ptr<Session> & session;
    Poco::Net::SocketAddress address;
    pcg64_fast gen;
    std::uniform_int_distribution<Int32> dis;
};

}

}
