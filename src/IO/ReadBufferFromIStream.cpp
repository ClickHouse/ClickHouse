#include <IO/ReadBufferFromIStream.h>
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

    if (!gcount)
    {
        if (istr.eof())
            return false;

        if (istr.fail())
            throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Cannot read from istream at offset {}", count());

        throw Exception(ErrorCodes::CANNOT_READ_FROM_ISTREAM, "Unexpected state of istream at offset {}", count());
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
