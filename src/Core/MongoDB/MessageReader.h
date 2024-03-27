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
    explicit MessageReader(ReadBuffer & reader_) : reader(reader_) { }

    Message::Ptr read()
    {
        RequestMessage::Ptr message;
        MessageHeader header;
        header.read(reader);
        switch (header.getOpCode())
        {
            case MessageHeader::OP_QUERY:
                message = new QueryRequest(header);
                break;
            default:
                throw Poco::NotImplementedException();
        }
        message->read(reader);
        return message;
    }

private:
    ReadBuffer & reader;
};


}
} // namespace DB::MongoDB
