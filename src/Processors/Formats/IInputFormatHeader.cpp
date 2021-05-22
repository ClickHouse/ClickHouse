#include <Processors/Formats/IInputFormatHeader.h>
#include <IO/ReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IInputFormatHeader::IInputFormatHeader(ReadBuffer & in_)
    : in(in_), header(nullptr)
{
}

IInputFormatHeader::IInputFormatHeader(const IInputFormatHeader & other)
    : in(other.in), header(other.header)
{
}

}
