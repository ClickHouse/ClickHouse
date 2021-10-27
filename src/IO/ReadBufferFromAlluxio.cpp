#include <Interpreters/Context.h>
#include <memory>

#include "ReadBufferFromAlluxio.h"

namespace DB
{

ReadBufferFromAlluxio::ReadBufferFromAlluxio(const String & host_, int port_, const String & file_, size_t buf_size_)
    : BufferWithOwnMemory<ReadBuffer>(buf_size_)
    , client(std::make_shared<AlluxioClient>(host_, port_))
    , file(file_)
    , id(client->openFile(file))
    , is(client->read(id))
{
    assert(client != nullptr);
    assert(id >= 0);
    assert(is != nullptr);
}

ReadBufferFromAlluxio::~ReadBufferFromAlluxio()
{
    client->close(id);
}


bool ReadBufferFromAlluxio::nextImpl()
{
    is->read(internal_buffer.begin(), internal_buffer.size());
    int bytes_read = is->gcount();

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;
    return true;
}

}
