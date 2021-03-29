#include <Core/Field.h>
#include <IO/ReadBufferFromIStream.h>
#include "common/logger_useful.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}

bool ReadBufferFromIStream::nextImpl()
{
    istr.read(internal_buffer.begin(), internal_buffer.size());
    size_t gcount = istr.gcount();
    LOG_DEBUG(&Poco::Logger::get("ReadBufferFromIStream"), "gcount={}, uuid ={} ", gcount, toString(uuid));

    already_read += gcount;
    if (!gcount)
    {
        if (istr.eof())
            return false;

        if (istr.fail())
             throw Exception(
                 "Cannot read from istream at offset " + std::to_string(count()) + toString(uuid), ErrorCodes::CANNOT_READ_FROM_ISTREAM);

        throw Exception("Unexpected state of istream at offset " + std::to_string(count()), ErrorCodes::CANNOT_READ_FROM_ISTREAM);
    }
    else
        working_buffer.resize(gcount);

    return true;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size)
    : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_)
{
}

}
