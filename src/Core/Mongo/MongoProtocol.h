#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include "base/types.h"

#include <bson/bson.h>

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

#if 0
std::string bsonToJson(const std::string& bsonData) {
    bson_t b;
    //bson_error_t error;

    if (!bson_init_static(&b, reinterpret_cast<const uint8_t*>(bsonData.data()), bsonData.size())) {
        throw std::runtime_error("Failed to initialize BSON data");
    }

    char* json_str = bson_as_json(&b, nullptr);
    if (!json_str) {
      // Try to get the error message and throw it
      //char *err_msg = bson_error_to_string(&error, "bson_as_canonical_extended_json() failed: ");
      //std::string error_message(err_msg);
      //bson_free(err_msg);
      throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect bson");
    }


    std::string json(json_str);
    bson_free(json_str);

    return json;
}
#endif

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

    void deserialize(ReadBuffer & in) override;
};

struct IsMasterResponse : public BackendMessage
{
    mutable Header header;
    const char * bson_data;
    UInt32 bson_len;

    IsMasterResponse(Int32 request_id_, Int32 /*response_to_*/);

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override;
};

struct OpSection : public FrontMessage
{
    UInt8 kind;
    String identifier;
    std::vector<String> documents;

    OpSection() = default;
    OpSection(OpSection && other) noexcept;

    void deserialize(ReadBuffer & in) override;
};

#if 0
struct OpMessage : public FrontMessage
{
    UInt32 flag_bits;
    std::vector<OpSection> sections;

    void deserialize(ReadBuffer & in) override
    {
        readBinaryLittleEndian(flag_bits, in);
        while (!in.eof()) 
        {
            OpSection section;
            section.deserialize(in);
            sections.push_back(std::move(section));
        }
    }
};
#endif
struct OpMessageResponse : public BackendMessage
{
    mutable Header header;
    const char * bson_data;
    UInt32 bson_len;
    UInt32 flags;

    OpMessageResponse(Int32 request_id_, UInt32 flags_);

    void serialize(WriteBuffer & out) const override;

    Int32 size() const override { return header.message_length; }
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

}

}
