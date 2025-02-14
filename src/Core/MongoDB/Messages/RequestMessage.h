#pragma once

#include "Message.h"
#include "MessageHeader.h"


namespace DB
{
namespace MongoDB
{


class RequestMessage : public Message
/// Base class for a request sent to the MongoDB server.
{
public:
    using Ptr = Poco::SharedPtr<RequestMessage>;

    explicit RequestMessage(const MessageHeader & header_) : Message(header_) { }

    ~RequestMessage() override { }
    /// Destroys the RequestMessage.

    virtual void read(ReadBuffer & reader) = 0;

    virtual std::string toString() const = 0;
};

}
}
