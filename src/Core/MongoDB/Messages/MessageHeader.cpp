#include "MessageHeader.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Loggers/Loggers.h>
#include <fmt/format.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace MongoDB
{

void MessageHeader::write(WriteBuffer & writer) const
{
    writeIntBinary(message_length, writer);
    writeIntBinary(request_id, writer);
    writeIntBinary(response_to, writer);
    writeIntBinary(static_cast<Int32>(op_code), writer);
}


void MessageHeader::read(ReadBuffer & reader)
{
    readIntBinary(message_length, reader);
    readIntBinary(request_id, reader);
    readIntBinary(response_to, reader);
    Int32 opcode;
    readIntBinary(opcode, reader);
    op_code = static_cast<OpCode>(opcode);
}


MessageHeader::MessageHeader(OpCode op_code_) : message_length(0), request_id(0), response_to(0), op_code(op_code_)
{
}


std::string MessageHeader::toString() const
{
    return fmt::format(
        "message_length: {}\n"
        "request_id : {}\n"
        "response_to: {}\n"
        "op_code: {}\n",
        message_length,
        request_id,
        response_to,
        static_cast<Int32>(op_code));
}

}
}
