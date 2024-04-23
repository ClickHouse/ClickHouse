#include <IO/WriteBufferFromString.h>
#include <Loggers/Loggers.h>
#include <Poco/Exception.h>
#include "Message.h"
#include "QueryRequest.h"


namespace DB
{
namespace MongoDB
{


class MessageReader
{
public:
    constexpr static const auto handler_name = "MessageReader";
    explicit MessageReader(ReadBuffer & reader_) : reader(reader_) { }

    Message::Ptr read()
    {
        RequestMessage::Ptr message;
        MessageHeader header;
        header.read(reader);
        LOG_INFO(log, "{}", header.toString());
        switch (header.getOpCode())
        {
            case MessageHeader::OP_QUERY:
                message = new QueryRequest(header);
                break;
            case MessageHeader::OP_MSG:
                message = new OpMsgMessage(header);
                break;
            default:
                LOG_INFO(log, "Unknown OpCode {}", static_cast<int>(header.getOpCode()));
                throw Poco::NotImplementedException();
        }
        LOG_INFO(log, "Parsed OPcode: {}, request_id: {}, starting reading", static_cast<Int32>(header.getOpCode()), header.getRequestID());
        message->read(reader);
        LOG_INFO(log, "Successfully read message: {}", message->toString());
        return message;
    }

private:
    ReadBuffer & reader;
    LoggerPtr log = getLogger(handler_name);
};


}
} // namespace DB::MongoDB
