#include <IO/WriteBufferFromOStream.h>
#include <Common/MemoryTracker.h>


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
        throw Exception("Cannot write to ostream at offset " + std::to_string(count()),
            ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
}

WriteBufferFromOStream::WriteBufferFromOStream(
    size_t size,
    char * existing_memory,
    size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(size, existing_memory, alignment)
{
}

WriteBufferFromOStream::WriteBufferFromOStream(
    std::ostream & ostr_,
    size_t size,
    char * existing_memory,
    size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(size, existing_memory, alignment), ostr(&ostr_)
{
}

WriteBufferFromOStream::~WriteBufferFromOStream()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    next();
}

}
