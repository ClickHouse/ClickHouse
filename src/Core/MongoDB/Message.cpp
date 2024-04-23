#include "Message.h"


namespace DB
{
namespace MongoDB
{


Message::~Message()
{
}

Message::Message(MessageHeader::OpCode opcode) : header(opcode)
{
}
}
} // namespace DB::MongoDB
