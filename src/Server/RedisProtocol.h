#pragma once

#include <Core/Types.h>

#include <cstddef>
#include <string_view>
#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

namespace RedisProtocol
{

struct Command
{
    String name;
    std::vector<String> arguments;
};

Command readCommand(ReadBuffer & in);

void writeSimpleString(WriteBuffer & out, std::string_view value);
void writeError(WriteBuffer & out, std::string_view value);
void writeBulkString(WriteBuffer & out, std::string_view value);
void writeNullBulkString(WriteBuffer & out);
void writeArrayHeader(WriteBuffer & out, size_t size);

}

}
