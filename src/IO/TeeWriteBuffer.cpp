#include <IO/TeeWriteBuffer.h>

namespace DB
{

TeeWriteBuffer::TeeWriteBuffer(
    const std::string & file_name_,
    size_t buf_size,
    int flags,
    mode_t mode,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFile(file_name_,buf_size,flags,mode,existing_memory,alignment),
      stdout_buffer(STDOUT_FILENO,buf_size,working_buffer.begin())
{
}

void TeeWriteBuffer::nextImpl()
{
    try
    {
        stdout_buffer.position() = position();
        stdout_buffer.next();
        WriteBufferFromFile::nextImpl();
    }
    catch (Exception &exception)
    {
        exception.addMessage("While writing to TeeWriteBuffer ");
        throw;
    }
}

void TeeWriteBuffer::finalizeImpl()
{
    if (fd < 0 || stdout_buffer.getFD() < 0)
        return;

    next();
}

TeeWriteBuffer::~TeeWriteBuffer()
{
    finalize();
}

}
