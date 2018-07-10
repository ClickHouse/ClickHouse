#include <IO/WriteBufferFromOStream.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_OSTREAM;
}

void WriteBufferFromOStream::nextImpl()
{
    if (!offset())
        return;

    ostr->write(working_buffer.begin(), offset());
    ostr->flush();

    if (!ostr->good())
        throw Exception("Cannot write to ostream at " + std::to_string(offset()) + "/" + std::to_string(size),
            ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
}

WriteBufferFromOStream::WriteBufferFromOStream(
    size_t size,
    char * existing_memory,
    size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(size, existing_memory, alignment)
    , size{size}
{
}

WriteBufferFromOStream::WriteBufferFromOStream(
    std::ostream & ostr_,
    size_t size,
    char * existing_memory,
    size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(size, existing_memory, alignment), ostr(&ostr_)
    , size{size}
{
}

WriteBufferFromOStream::~WriteBufferFromOStream()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
