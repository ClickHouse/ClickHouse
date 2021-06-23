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
    size_t gcount = 0;
    if (offset && offset - read_pos > internal_buffer.size())
    {
        gcount = internal_buffer.size();
    }
    else if (read_pos < offset)
    {
        gcount = offset - read_pos;
    }
    else
    {
        istr.read(internal_buffer.begin(), internal_buffer.size());
        gcount = istr.gcount();
    }
    read_pos += gcount;

    if (!gcount)
    {
        if (istr.eof())
            return false;

        if (istr.fail())
            throw Exception("Cannot read from istream at offset " + std::to_string(count()), ErrorCodes::CANNOT_READ_FROM_ISTREAM);

        throw Exception("Unexpected state of istream at offset " + std::to_string(count()), ErrorCodes::CANNOT_READ_FROM_ISTREAM);
    }
    else
        working_buffer.resize(gcount);

    return true;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size, size_t offset_)
    : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_), offset(offset_)
{
}

}
