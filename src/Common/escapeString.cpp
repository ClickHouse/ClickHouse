#include <Common/escapeString.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

String escapeString(std::string_view value)
{
    WriteBufferFromOwnString buf;
    writeEscapedString(value, buf);
    return buf.str();
}

}
