#pragma once

#include <base/types.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{
namespace MongoDB
{


    class Message; // Required to disambiguate friend declaration in MessageHeader.


    class MessageHeader
    /// Represents the message header which is always prepended to a
    /// MongoDB request or response message.
    {
    public:
        static const unsigned int MSG_HEADER_SIZE = 16;

        enum OpCode
        {
            // Opcodes deprecated in MongoDB 5.0
            OP_REPLY = 1,
            OP_UPDATE = 2001,
            OP_INSERT = 2002,
            OP_QUERY = 2004,
            OP_GET_MORE = 2005,
            OP_DELETE = 2006,
            OP_KILL_CURSORS = 2007,

            /// Opcodes supported in MongoDB 5.1 and later
            OP_COMPRESSED = 2012,
            OP_MSG = 2013
        };

        MessageHeader() = default;

        explicit MessageHeader(OpCode);
        /// Creates the MessageHeader using the given OpCode.

        MessageHeader(const MessageHeader & other) = default;
        MessageHeader(MessageHeader&& other) = default;

        virtual ~MessageHeader() = default;
        /// Destroys the MessageHeader.

        void read(ReadBuffer & reader);
        /// Reads the header using the given ReadBuffer.

        void write(WriteBuffer & writer) const;
        /// Writes the header using the given WriteBuffer.

        Int32 getMessageLength() const;
        /// Returns the message length.

        OpCode getOpCode() const;
        /// Returns the OpCode.

        Int32 getRequestID() const;
        /// Returns the request ID of the current message.

        void setRequestID(Int32 id);
        /// Sets the request ID of the current message.

        Int32 getResponseTo() const;
        /// Returns the request id from the original request.

        void setResponseTo(Int32 response_to_);

    private:
        void setContentLength(Int32 length);
        /// Sets the message length.

        Int32 message_length;
        Int32 request_id;
        Int32 response_to;
        OpCode op_code;

        friend class Message;
    };


    //
    // inlines
    //
    inline MessageHeader::OpCode MessageHeader::getOpCode() const
    {
        return op_code;
    }


    inline Int32 MessageHeader::getMessageLength() const
    {
        return message_length;
    }


    inline void MessageHeader::setContentLength(Int32 length)
    {
        message_length = MSG_HEADER_SIZE + length;
    }


    inline void MessageHeader::setRequestID(Int32 id)
    {
        request_id = id;
    }


    inline Int32 MessageHeader::getRequestID() const
    {
        return request_id;
    }

    inline Int32 MessageHeader::getResponseTo() const
    {
        return response_to;
    }

    inline void MessageHeader::setResponseTo(Int32 response_to_) {
        response_to = response_to_;
    }

    void MessageHeader::write(WriteBuffer& writer) const
    {
        writeIntBinary(message_length, writer);
        writeIntBinary(request_id, writer);
        writeIntBinary(response_to, writer);
        writeIntBinary(static_cast<Int32>(op_code), writer);
    }


    void MessageHeader::read(ReadBuffer& reader)
    {
        readIntBinary(message_length, reader);
        readIntBinary(request_id, reader);
        readIntBinary(response_to, reader);
        Int32 opcode;
        readIntBinary(opcode, reader);
        op_code = static_cast<OpCode>(opcode);
    }




    MessageHeader::MessageHeader(OpCode op_code_):
        message_length(0),
        request_id(0),
        response_to(0),
        op_code(op_code_)
    {
    }


}
} // namespace DB::MongoDB
