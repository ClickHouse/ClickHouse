#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <base/types.h>

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

/// The largest message we are willing to read from the wire. It matches the
/// `maxMessageSizeBytes` we advertise in the reply to `isMaster`, so a well behaved
/// client never sends anything bigger.
static constexpr UInt32 MAX_MESSAGE_SIZE = 48000000;

/// The largest BSON document we are willing to produce or accept, which is the
/// `maxBsonObjectSize` we advertise. A reply is itself one BSON document, so a result
/// that does not fit must be rejected rather than sent: there is no cursor to continue
/// from - every reply carries the whole result and a cursor id of 0.
static constexpr UInt32 MAX_BSON_OBJECT_SIZE = 16777216;

/// The smallest possible BSON document: a 4-byte length and the terminating zero byte.
static constexpr UInt32 MIN_DOCUMENT_SIZE = 5;

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
    /// Size of the header on the wire.
    static constexpr Int32 SIZE = 16;

    UInt32 message_length = 0;
    UInt32 request_id = 0;
    UInt32 response_to = 0;
    UInt32 operation_code = 0;

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
    std::unique_ptr<TMessage> receive()
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>();
        message->deserialize(*in);
        return message;
    }

    /** Reads the payload of a single message (everything after the header) into a string.
     * Mongo messages are length-delimited by `Header::message_length`, and a single TCP read
     * may return a part of a message or several messages at once. Parsing the payload from
     * its own bounded buffer is what keeps message boundaries intact.
     */
    String receivePayload(const Header & header);

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

    void flush() { out->next(); }

    Int32 getNextResponseId() { return ++response_counter; }
};

class QueryExecutor
{
public:
    explicit QueryExecutor(std::unique_ptr<Session> & session_, const Poco::Net::SocketAddress & address_);

    String execute(const String & query);

    void authenticate(const String & username, const String & password);

    /// The name of the user this connection has authenticated as, or an empty string before a
    /// successful `saslStart`.
    String getAuthenticatedUserName() const;

private:
    std::unique_ptr<Session> & session;
    Poco::Net::SocketAddress address;
    pcg64_fast gen;
    std::uniform_int_distribution<Int32> dis;
};

}

}
