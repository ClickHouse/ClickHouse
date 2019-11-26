#include <Processors/Formats/IInputFormat.h>
#include <IO/WriteHelpers.h>    // toString


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IInputFormat::IInputFormat(Block header, ReadBuffer & in_)
    : ISource(std::move(header)), in(in_)
{
}


void IInputFormat::resetParser()
{
    if (in.hasPendingData())
        throw Exception("Unread data in IInputFormat::resetParser. Most likely it's a bug.", ErrorCodes::LOGICAL_ERROR);
}

}
