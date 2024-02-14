#pragma once

#include "MessageHeader.h"
#include <Poco/SharedPtr.h>
#include <IO/ReadBuffer.h>


namespace DB
{
namespace MongoDB
{


    class Message
    /// Base class for all messages send or retrieved from MongoDB server.
    {
    public:

        using Ptr = Poco::SharedPtr<Message>;

        explicit Message(MessageHeader::OpCode opcode);
        /// Creates a Message using the given OpCode.

        explicit Message(const MessageHeader & header_) : header(header_) {}

        virtual ~Message();
        /// Destructor

        MessageHeader & getHeader();
        /// Returns the message header

        const MessageHeader & getHeader() const;

    protected:
        MessageHeader header;

        void setContentLength(Int32 length);
        /// Sets the message length in the message header
    };


    Message::~Message() {}
    //
    // inlines
    //
    inline MessageHeader & Message::getHeader()
    {
        return header;
    }

    inline const MessageHeader & Message::getHeader() const
    {
        return header;
    }


    inline void Message::setContentLength(Int32 length)
    {
        poco_assert(length > 0);
        header.setContentLength(length);
    }

Message::Message(MessageHeader::OpCode opcode):
	header(opcode)
{
}


}
} // namespace Poco::MongoDB
