#include <Disks/IO/WriteBufferFromRemoteDisk.h>

namespace DB
{

WriteBufferFromRemoteDisk::WriteBufferFromRemoteDisk(
    RemoteFSConnectionPool::Entry & conn_,
    std::string file_name_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment)
    , conn(conn_)
    , file_name(std::move(file_name_))
{
}

WriteBufferFromRemoteDisk::~WriteBufferFromRemoteDisk()
{
    finalize();
}

void WriteBufferFromRemoteDisk::finalizeImpl()
{
    next();
    conn->endWriteFile();
}

void WriteBufferFromRemoteDisk::nextImpl()
{
    conn->writeData(working_buffer.begin(), offset());
}

std::string WriteBufferFromRemoteDisk::getFileName() const
{
    return file_name;
}

void WriteBufferFromRemoteDisk::sync()
{
    next();
}

}
